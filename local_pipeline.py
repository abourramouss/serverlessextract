# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
from abc import ABC, abstractmethod
import os
from typing import List, Union
import lithops
from typing import Dict, Optional
from lithops import Storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3path import S3Path
import pickle
from pathlib import PurePosixPath, PosixPath
import subprocess as sp


def dict_to_parset(
    data, output_dir=PosixPath("/tmp"), filename="output.parset"
) -> PosixPath:
    lines = []

    for key, value in data.items():
        # Check if the value is another dictionary
        if isinstance(value, dict):
            lines.append(f"[{key}]")
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):  # Check for nested dictionaries
                    lines.append(f"[{sub_key}]")
                    for sub_sub_key, sub_sub_value in sub_value.items():
                        lines.append(f"{sub_sub_key} = {sub_sub_value}")
                else:
                    lines.append(f"{sub_key} = {sub_value}")
        else:
            lines.append(f"{key} = {value}")

    # Convert the list of lines to a single string
    parset_content = "\n".join(lines)

    # Ensure the directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / filename
    with output_path.open("w") as file:
        file.write(parset_content)

    return output_path


def s3_to_local_path(
    s3_path: S3Path, base_local_dir: PurePosixPath = PurePosixPath("/tmp")
) -> PurePosixPath:
    """Converts an S3Path to a local file path."""
    local_path = os.path.join(base_local_dir, s3_path.bucket, s3_path.key)
    return PurePosixPath(local_path)


def local_to_s3_path(local_path: str, base_local_dir: str = "/tmp") -> S3Path:
    """Converts a local file path to an S3Path."""
    local_path = os.path.abspath(local_path)
    components = local_path.replace(base_local_dir, "").split(os.path.sep)[1:]
    bucket = components[0]
    key = os.path.join(*components[1:])
    return S3Path(f"{bucket}/{key}")


class PipelineStep(ABC):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters: Dict,
        output: Optional[Dict[str, S3Path]] = None,
    ):
        self._input_data_path = input_data_path
        self._parameters = parameters
        self._output = output

    @property
    @abstractmethod
    def input_data_path(self) -> List[S3Path]:
        pass

    @property
    @abstractmethod
    def parameters(self) -> Dict:
        pass

    @property
    @abstractmethod
    def output(self) -> S3Path:
        pass

    @abstractmethod
    def build_command(self, *args, **kwargs):
        pass


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
        self, read_path: S3Path, write_path: PurePosixPath = PurePosixPath("/tmp")
    ) -> PurePosixPath:
        """Download a file from S3 and returns the local path."""
        if isinstance(read_path, S3Path):
            try:
                local_path = s3_to_local_path(read_path, base_local_dir=str(write_path))
                print("local_path:", local_path)
                os.makedirs(local_path.parent, exist_ok=True)
                self.storage.download_file(
                    read_path.bucket, read_path.key, str(local_path)
                )
                return local_path
            except Exception as e:
                print(f"Failed to download file {read_path.key}: {e}")

    def download_directory(
        self, read_path: S3Path, write_path: PurePosixPath = PurePosixPath("/tmp")
    ) -> PurePosixPath:
        """Download a directory from S3 and returns the local path."""
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(write_path)
        )

        for key in keys:
            s3_file_path = S3Path.from_bucket_key(read_path.bucket, key)
            local_file_path = s3_to_local_path(
                s3_file_path, base_local_dir=str(write_path)
            )
            os.makedirs(local_file_path.parent, exist_ok=True)
            self.download_file(s3_file_path, local_file_path)

        return local_directory_path

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
        parameters: Dict,
        output: Optional[Dict[str, S3Path]] = None,
    ):
        super().__init__(input_data_path, parameters, output)

    @property
    def input_data_path(self) -> List[S3Path]:
        return self._input_data_path

    @property
    def parameters(self) -> Dict:
        return self._parameters

    @property
    def output(self) -> S3Path:
        return self._output

    def run(self):
        extra_env = {"HOME": "/tmp"}
        function_executor = lithops.FunctionExecutor()

        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path["ms"].bucket,
            prefix=self.input_data_path["ms"].key,
        )

        # Create an empty set to hold unique directories
        unique_partitions = set()

        # Iterate over each key
        for key in keys:
            # Split the key into its parts
            parts = key.split("/")

            # Extract the directory that ends in .ms
            partition = next((part for part in parts if part.endswith(".ms")), None)
            if partition:
                # Combine the prefix with the partition
                full_partition_path = "/".join(parts[: parts.index(partition) + 1])
                unique_partitions.add(full_partition_path)

        s3_paths = {
            (
                S3Path.from_bucket_key(
                    bucket=self.input_data_path["ms"].bucket, key=partition
                ),
                pickle.dumps(self.parameters),
                self.output["calibrated_ms"],
            )
            for partition in unique_partitions
        }

        futures = function_executor.map(
            self.build_command,
            s3_paths,
            extra_env=extra_env,
        )
        results = function_executor.get_result(futures)
        return results

    def build_command(
        self, ms: S3Path, parameters: str, calibrated_ms: S3Path
    ) -> List[str]:
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        partition_path = data_source.download_directory(ms)
        param_path = dict_to_parset(params["flagrebin"])
        aoflag_path = data_source.download_file(params["flagrebin"]["aoflag.strategy"])

        cmd = [
            "DP3",
            param_path,
            f"msin={partition_path}",
            f"apply.parmdb={aoflag_path}",
        ]

        proc = sp.Popen(cmd)
        print(partition_path)
        print(param_path)
        print(aoflag_path)


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

    parameters = {
        "RebinningStep": {
            "input_data_path": {"ms": S3Path("/extract/partitions/partitions")},
            "parameters": {
                "flagrebin": {
                    "msin": "",
                    "msout": "",
                    "steps": "[aoflag, avg, count]",
                    "aoflag.type": "aoflagger",
                    "aoflag.memoryperc": 90,
                    "aoflag.strategy": S3Path(
                        "/extract/parameters/rebinning/STEP1-NenuFAR64C1S.lua"
                    ),
                    "avg.type": "averager",
                    "avg.freqstep": 4,
                    "avg.timestep": 8,
                }
            },
            "output": {
                "calibrated_ms": S3Path(
                    "/aymanb-serverless-genomics/extract-data/rebinning_out/"
                )
            },
        }
    }

    RebinningStep(
        input_data_path=parameters["RebinningStep"]["input_data_path"],
        parameters=parameters["RebinningStep"]["parameters"],
        output=parameters["RebinningStep"]["output"],
    ).run()
