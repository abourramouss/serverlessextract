# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
from abc import ABC, abstractmethod
import os
import subprocess as sp
import psutil
import time
import matplotlib.pyplot as plt
from typing import List, Union, Tuple, Callable
import shutil
import lithops
from typing import Dict, Optional
from lithops import Storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3path import S3Path
from util import (
    rebinning_param_parset,
    cal_param_parset,
    sub_param_parset,
    apply_cal_param_parset,
)
from pathlib import PurePosixPath
import multiprocessing


def s3_to_local_path(s3_path: S3Path, base_local_dir: str = "/tmp") -> str:
    """Converts an S3Path to a local file path."""
    local_path = os.path.join(base_local_dir, s3_path.bucket, s3_path.key)
    return local_path


def local_to_s3_path(local_path: str, base_local_dir: str = "/tmp") -> S3Path:
    """Converts a local file path to an S3Path."""
    local_path = os.path.abspath(local_path)
    bucket, key = local_path.replace(base_local_dir, "").split(os.path.sep)[1:]
    return S3Path(f"{bucket}/{key}")


class PipelineStep(ABC):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters_path: Dict[str, S3Path],
        output: Optional[Dict[str, S3Path]] = None,
    ):
        self._input_data_path = input_data_path
        self._parameters_path = parameters_path
        self._output = output

    @property
    @abstractmethod
    def input_data_path(self) -> List[S3Path]:
        pass

    @property
    @abstractmethod
    def parameters_path(self) -> List[S3Path]:
        pass

    @property
    @abstractmethod
    def output(self) -> S3Path:
        pass

    @abstractmethod
    def build_command(self, *args, **kwargs):
        pass


# TODO: Problem: Executor methods aren't generic, this makes it hard to execute things
# like preprocessing before the step execution, create a more generic interface and
# implement it in the executors, one idea is just a Callable and set of params
"""
class LithopsExecutor(Executor):
    def __init__(self):
        self._executor = lithops.FunctionExecutor()

    def compute(self, runner: Callable, parameters: Union[dict, list]) -> None:
        self._executor.call_async(runner, parameters)
        self._executor.get_result()

#Usage:
def foo():
    download()
    execute()
    ...
self._executor.compute(foo)

Instead of:
self._executor.execute_steps(steps, parameters) <- This is not generic
"""


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    @abstractmethod
    def download_file(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        pass

    @abstractmethod
    def download_directory(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        pass

    @abstractmethod
    def upload_file(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        pass

    @abstractmethod
    def upload_directory(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        pass

    def write_parset_dict_to_file(self, parset_dict: dict, filename: str):
        with open(filename, "w") as f:
            for key, value in parset_dict.items():
                f.write(f"{key}={value}\n")


class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()

    def download_file(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        if isinstance(read_path, S3Path):
            try:
                os.makedirs(os.path.dirname(write_path), exist_ok=True)
                self.storage.download_file(read_path.bucket, read_path.key, write_path)
            except Exception as e:
                print(f"Failed to download file {read_path.key}: {e}")

    def download_directory(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(self.download_file, read_path, write_path)
                for key in keys
            ]
        for future in as_completed(futures):
            future.result()

    def upload_file(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        if isinstance(write_path, S3Path):
            try:
                self.storage.upload_file(read_path, write_path.bucket, write_path.key)
            except:
                print(f"Failed to upload file {read_path} to {write_path}")

    def upload_directory(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        files = [file for file in os.walk(read_path)]
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(self.upload_file, file, write_path) for file in files
            ]
        for future in as_completed(futures):
            future.result()


# TODO: Enable ingestion of chunks or entire ms, rebinning measurement_set could potentially be a list of ms
# TODO: Refactor rebinning step in Lithops version, lua_file_path is not used,
# it's directly loaded from the parameter_file_path should be checked if it exists or removed
# TODO: DONE Enable dynamic loading/linking of the parameter files, this means creating them on runtime,
# or downloading-modifying them also on runtime.
# TODO: Check input parameters for the Pipeline class, not all of them are needed
# TODO: Abstract parameter dict logic to a class, to handle S3 and posix fs.


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
# Outputs:
#   - write_path: creates a new measurement set in the write path
class RebinningStep(PipelineStep):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters_path: Dict[str, S3Path],
        output: Optional[Dict[str, S3Path]] = None,
    ):
        super().__init__(input_data_path, parameters_path, output)

    @property
    def input_data_path(self) -> List[S3Path]:
        return self._input_data_path

    @property
    def parameters_path(self) -> List[S3Path]:
        return self._parameters_path

    @property
    def output(self) -> S3Path:
        return self._output

    def build_command(self, ms, calibrated_ms, flagrebin) -> List[str]:
        data_source = LithopsDataSource()

        print(f"Rebinning {ms}")
        print(f"Calibrated ms: {calibrated_ms}")
        print(f"Flag rebin: {flagrebin}")

    def run(self):
        function_executor = lithops.FunctionExecutor()

        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path["ms"].bucket,
            prefix=self.input_data_path["ms"].key,
        )
        partitions = set()
        for key in keys:
            partition = "/".join(key.split("/")[:3])
            if partition.endswith(".ms"):
                partitions.add(partition)

        result = [
            {
                "ms": partition,
                "calibrated_ms": self.output["calibrated_ms"],
                "flagrebin": self.parameters_path["flagrebin"],
            }
            for partition in partitions
        ]

        function_executor.map(
            self.build_command,
            result,
        )


if __name__ == "__main__":
    # Pipeline
    # Inputs:
    #   - measurement_set: path to the measurement set
    # Outputs:
    #   - write_path: creates a new measurement set in the write path
    # Obligatory parameters needed in the pipeline:
    #   - measurement_set: path to the measurement set (uncalibrated).
    #   - calibrated_measurement_set: path to the calibrated measurement set (where should rebinning write)
    #   - image_output_path: path to the output directory where the .fits files will be saved
    #   - parameter_file_path: path to the parameter file for each step

    # Possible S3 Paths:
    # remote_ms = "s3://aymanb-serverless-genomics/extract-data/partitions_60/SB205/SB205.MS"
    # remote_lua_file_path = "s3://aymanb-serverless-genomics/extract-data/parameters/rebinning.lua"
    # remote_sourcedb_directory = "s3://aymanb-serverless-genomics/extract-data/parameters/apparent.sourcedb"
    # remote_calibrated_ms_imaging = "s3://aymanb-serverless-genomics/pipeline/SB205.ms"
    # remote_image_output_path = "s3://aymanb-serverless-genomics/pipeline/OUTPUT/Cygloop-205-210-b0-1024

    BUCKET_NAME = "aymanb-serverless-genomics"
    """
    # S3 paths
    ms = S3Path(
        "s3://aymanb-serverless-genomics/extract-data/partitions_5/partition_1.ms"
    )  # Can be the local or S3 Path
    calibrated_ms = "/home/ayman/Downloads/pipeline/SB205.ms"  # Local path of where the calibrated ms is being modified
    upload_calibrated_ms = (
        "/home/ayman/Downloads/pipeline/SB205.ms"  # Can be the local or S3 Path
    )
    h5 = "/home/ayman/Downloads/pipeline/cal_out/output.h5"  # Local path of the h5 file
    image_output_path = "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"  # Local path of the image file
    # Points to where the parameter files are located
    parameters_write_path = "/home/ayman/Downloads/pipeline/parameters"
    sourcedb_directory = f"{parameters_write_path}/cal/STEP2A-apparent.sourcedb"  # Local or S3 path of the sourcedb directory
    """
    # Local paths
    ms = "/home/ubuntu/partition_1.ms"  # Can be the local or S3 Path
    calibrated_ms = "/home/ubuntu/Downloads/pipeline/SB205.ms"  # Local path of where the calibrated ms is being modified
    upload_calibrated_ms = (
        "/home/ubuntu/Downloads/pipeline/SB205.ms"  # Can be the local or S3 Path
    )
    h5 = (
        "/home/ubuntu/Downloads/pipeline/cal_out/output.h5"  # Local path of the h5 file
    )
    image_output_path = "/home/ubuntu/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"  # Local path of the image file
    # Points to where the parameter files are located
    parameters_write_path = "/home/ubuntu/Downloads/pipeline/parameters"
    sourcedb_directory = f"{parameters_write_path}/cal/STEP2A-apparent.sourcedb"  # Local or S3 path of the sourcedb directory

    parameters = {
        "RebinningStep": {
            "input_data_path": {"ms": S3Path("/extract/partitions/partitions")},
            "parameters_path": {
                "flagrebin": S3Path(
                    "/aymanb-serverless-genomics/extract-data/rebinning/STEP1-flagrebin.parset"
                )
            },
            "output": {
                "calibrated_ms": S3Path(
                    "/aymanb-serverless-genomics/extract-data/rebinning_out/"
                )
            },
        }
    }

    print(parameters["RebinningStep"]["input_data_path"]["ms"].bucket)
    RebinningStep(
        input_data_path=parameters["RebinningStep"]["input_data_path"],
        parameters_path=parameters["RebinningStep"]["parameters_path"],
        output=parameters["RebinningStep"]["output"],
    ).run()
