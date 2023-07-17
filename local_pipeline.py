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
from lithops import Storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from util.helpers import S3Path, rebinning_param_parset, cal_param_parset, sub_param_parset, apply_cal_param_parset
from pathlib import PurePosixPath
import multiprocessing
import os
import shutil
from pathlib import PurePath, _PosixFlavour
import logging
from contextlib import suppress

rebinning_param_parset = {
    "msin": "",
    "msout": "",
    "steps": "[aoflag, avg, count]",
    "aoflag.type": "aoflagger",
    "aoflag.memoryperc": 90,
    "avg.type": "averager",
    "avg.freqstep": 4,
    "avg.timestep": 8,
}

cal_param_parset = {
    "msin": "",
    "msin.datacolumn": "DATA",
    "msout": ".",
    "steps": "[cal]",
    "cal.type": "ddecal",
    "cal.mode": "diagonal",
    "cal.h5parm": "",
    "cal.solint": 4,
    "cal.nchan": 4,
    "cal.maxiter": 50,
    "cal.uvlambdamin": 5,
    "cal.smoothnessconstraint": 2e6,
}

sub_param_parset = {
    "msin": "",
    "msin.datacolumn": "DATA",
    "msout": ".",
    "msout.datacolumn": "SUBTRACTED_DATA",
    "steps": "[sub]",
    "sub.type": "h5parmpredict",
    "sub.directions": "[[CygA], [CasA]]",
    "sub.operation": "subtract",
    "sub.applycal.parmdb": "",
    "sub.applycal.steps": "[sub_apply_amp, sub_apply_phase]",
    "sub.applycal.correction": "fulljones",
    "sub.applycal.sub_apply_amp.correction": "amplitude000",
    "sub.applycal.sub_apply_phase.correction": "phase000",
}

apply_cal_param_parset = {
    "msin": "",
    "msin.datacolumn": "SUBTRACTED_DATA",
    "msout": ".",
    "msout.datacolumn": "CORRECTED_DATA",
    "steps": "[apply]",
    "apply.type": "applycal",
    "apply.steps": "[apply_amp, apply_phase]",
    "apply.apply_amp.correction": "amplitude000",
    "apply.apply_phase.correction": "phase000",
    "apply.direction": "[Main]",
    "apply.parmdb": "",
}
class _S3Flavour(_PosixFlavour):
    is_supported = True

    def parse_parts(self, parts):
        drv, root, parsed = super().parse_parts(parts)
        for part in parsed[1:]:
            if part == "..":
                index = parsed.index(part)
                parsed.pop(index - 1)
                parsed.remove(part)
        return drv, root, parsed

    def make_uri(self, path):
        uri = super().make_uri(path)
        return uri.replace("file:///", "s3://")


class S3Path(PurePath):
    """
    PurePath subclass for AWS S3 service
    Source: https://github.com/liormizr/s3path
    S3 is not a file-system, but we can look at it like a POSIX system
    """

    _flavour = _S3Flavour()
    __slots__ = ()

    @classmethod
    def from_uri(cls, uri: str) -> "S3Path":
        """
        from_uri class method create a class instance from url

        >> from s3path import PureS3Path
        >> PureS3Path.from_url('s3://<bucket>/<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        if not uri.startswith("s3://"):
            raise ValueError("Provided uri seems to be no S3 URI!")
        return cls(uri[4:])

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str) -> "S3Path":
        """
        from_bucket_key class method create a class instance from bucket, key pair's

        >> from s3path import PureS3Path
        >> PureS3Path.from_bucket_key(bucket='<bucket>', key='<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        bucket = cls(cls._flavour.sep, bucket)
        if len(bucket.parts) != 2:
            raise ValueError(
                "bucket argument contains more then one path element: {}".format(bucket)
            )
        key = cls(key)
        if key.is_absolute():
            key = key.relative_to("/")
        return bucket / key

    @property
    def bucket(self) -> str:
        """
        The AWS S3 Bucket name, or ''
        """
        self._absolute_path_validation()
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ""

    @property
    def key(self) -> str:
        """
        The AWS S3 Key name, or ''
        """
        self._absolute_path_validation()
        key = self._flavour.sep.join(self.parts[2:])
        return key

    @property
    def virtual_directory(self) -> str:
        """
        The parent virtual directory of a key
        Example: foo/bar/baz -> foo/baz
        """
        vdir, _ = self.key.rsplit("/", 1)
        return vdir

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        return super().as_uri()

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError("relative path have no bucket, key specification")

    def __repr__(self) -> str:
        return "{}(bucket={},key={})".format(
            self.__class__.__name__, self.bucket, self.key
        )


class PipelineStep(ABC):
    @abstractmethod
    def build_command(self, *args, **kwargs):
        pass

    def __call__(self, params):
        cmd = self.build_command(**params)
        print(f"Running {self.__class__.__name__}")
        stats = Pipeline.execute_command(cmd)
        Pipeline.plot_stats(stats, "stats", self.__class__.__name__)
        return stats


class Executor(ABC):
    @abstractmethod
    def execute(self, runner: Callable, parameters) -> None:
        pass


class LocalExecutor(Executor):
    def execute(self, runner: Callable, parameters) -> None:
        runner(parameters)


class LithopsExecutor(Executor):
    def __init__(self):
        self._executor = lithops.FunctionExecutor()

    #Call execute as a single call async function
    def execute(self, runner: Callable, parameters) -> None:
        futures = self._executor.call_async(runner, parameters)
        self._executor.get_result(fs=futures)

    

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

def remove_cached(parameters):
    for param in parameters:
        if "parameter_file_path" in parameters[param]:
            if os.path.exists(parameters[param]["parameter_file_path"]):
                os.remove(parameters[param]["parameter_file_path"])

    if os.path.exists(parameters[RebinningStep.__name__]["write_path"]):
        shutil.rmtree(parameters[RebinningStep.__name__]["write_path"])

    if os.path.exists(parameters[CalibrationStep.__name__]["output_h5"]):
        os.remove(parameters[CalibrationStep.__name__]["output_h5"])

    if os.path.exists(
        os.path.dirname(parameters[ImagingStep.__name__]["output_dir"])
    ):
        images = os.listdir(
            os.path.dirname(parameters[ImagingStep.__name__]["output_dir"])
        )
        for image in images:
            os.remove(
                os.path.join(
                    os.path.dirname(parameters[ImagingStep.__name__]["output_dir"]),
                    image,
                )
            )

def write_parset_dict_to_file(parset_dict: dict, filename: str):
    
    filename = f"/tmp{filename}"
    print(f"Writing parset file to {filename}")
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w") as f:
        for key, value in parset_dict.items():
            f.write(f"{key}={value}\n")


class LocalDataSource(DataSource):
    def download_file(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        pass

    def download_directory(
        self, read_path: Union[S3Path, PurePosixPath], write_path: PurePosixPath
    ) -> None:
        pass

    def upload_file(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        pass

    def upload_directory(
        self, read_path: PurePosixPath, write_path: Union[S3Path, PurePosixPath]
    ) -> None:
        pass


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
# TODO: Modify the step 1 dynamically so each time the parameters are changed, the lua file is updated!!!
# TODO: Check input parameters for the Pipeline class, not all of them are needed
# TODO: Abstract parameter dict logic to a class, to handle S3 and posix fs.


# Enables transparent execution of the pipeline in local or cloud enviroments
class Pipeline:
    @staticmethod
    def execute_command(cmd: List[str]) -> List[List[float]]:
        stats = []
        proc = sp.Popen(cmd)
        p = psutil.Process(proc.pid)
        while proc.poll() is None:
            try:
                cpu_usage = p.cpu_percent(interval=1)
                mem_usage = p.memory_info().rss / 1024**2
                # print(f"CPU usage: {cpu_usage/num_cores}")
                # print(f"Memory usage: {mem_usage} MB")
                stats.append([cpu_usage, mem_usage])
            except psutil.NoSuchProcess:
                print("Process finished")
                break
            time.sleep(1)
        return stats

    @staticmethod
    def plot_stats(stats: List[List[float]], subfolder: str, class_name: str) -> None:
        # Transpose the list of lists
        cpu_stats, mem_stats = zip(*stats)

        plt.figure()

        # Plot CPU stats
        plt.subplot(2, 1, 1)
        plt.plot(cpu_stats, label="CPU usage")
        plt.xlabel("Time (s)")
        plt.ylabel("CPU usage (%)")
        plt.legend()

        # Plot memory stats
        plt.subplot(2, 1, 2)
        plt.plot(mem_stats, label="Memory usage")
        plt.xlabel("Time (s)")
        plt.ylabel("Memory usage (MB)")
        plt.legend()

        plt.tight_layout()

        # Create the stats directory if it doesn't already exist
        if not os.path.exists("stats"):
            os.makedirs("stats")

        # Create the subfolder inside the stats directory if it doesn't already exist
        if not os.path.exists(f"stats/{subfolder}"):
            os.makedirs(f"stats/{subfolder}")

        # Save the figure to the specified subfolder with the class_name
        plt.savefig(f"stats/{subfolder}/{class_name}.png")

        plt.close()

    def __init__(
        self,
        parameters: dict,
        executor: Executor,
        datasource: DataSource,
    ) -> None:
        self.parameters = parameters
        self.steps = [
            RebinningStep(),
            CalibrationStep(),
            SubstractionStep(),
            ApplyCalibrationStep(),
            ImagingStep(),
        ]

        self._executor = executor
        self._datasource = datasource
    @staticmethod
    def download_ms(parameters):
        Pipeline._datasource.download_directory(
            parameters[RebinningStep.__name__]["measurement_set"],
            parameters[RebinningStep.__name__]["write_path"],
        )
    
    @staticmethod
    def prepare_parameters(parameters):
        # Create/modify parset files in the worker and download the lua and source db files.
        write_parset_dict_to_file(
            rebinning_param_parset,
            parameters[RebinningStep.__name__]["parameter_file_path"],
        )
        write_parset_dict_to_file(
            cal_param_parset,
            parameters[CalibrationStep.__name__]["parameter_file_path"],
        )
        write_parset_dict_to_file(
            sub_param_parset,
            parameters[SubstractionStep.__name__]["parameter_file_path"],
        )
        write_parset_dict_to_file(
            apply_cal_param_parset,
            parameters[ApplyCalibrationStep.__name__]["parameter_file_path"],
        )

        
    @staticmethod
    def execute_steps(steps) -> None:
        for step in steps:
            step(parameters[step.__class__.__name__])
    
    
    @staticmethod
    def run_pipeline(parameters):
        Pipeline.prepare_parameters(parameters)
        
        

    # Depending on the executor and datasource, we should prepare the parameters for execution i.e download ms and parameter files

    def run(self):
        self._executor.execute(Pipeline.run_pipeline
                               , self.parameters)
        # self.run_imaging()


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
# Outputs:
#   - write_path: creates a new measurement set in the write path
class RebinningStep(PipelineStep):
    def build_command(
        self, measurement_set: str, parameter_file_path: str, lua_file_dir: str, write_path: str
    ) -> List[str]:
        return [
            "DP3",
            parameter_file_path,
            f"msin={measurement_set}",
            f"msout={write_path}",
            f"aoflag.strategy={lua_file_dir}",
        ]


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
#   - sourcedb_directory: path to the sourcedb directory
# Outputs:
#   - output_h5: creates new h5 file in the output_h5 path
class CalibrationStep(PipelineStep):
    def build_command(
        self,
        calibrated_measurement_set: str,
        parameter_file_path: str,
        output_h5: str,
        sourcedb_directory: str,
    ) -> List[str]:
        return [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_measurement_set}",
            f"cal.h5parm={output_h5}",
            f"cal.sourcedb={sourcedb_directory}",
        ]


# Inputs:
#   - calibrated_measurement_set: path to the calibrated measurement set
#   - parameter_file_path: path to the parameter file
#   - sourcedb_directory: path to the sourcedb directory
# Outputs:
#   - output_h5: creates new h5 file in the output_h5 path
class SubstractionStep(PipelineStep):
    def build_command(
        self,
        calibrated_measurement_set: str,
        parameter_file_path: str,
        input_h5: str,
        sourcedb_directory: str,
    ) -> List[str]:
        return [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_measurement_set}",
            f"sub.applycal.parmdb={input_h5}",
            f"sub.sourcedb={sourcedb_directory}",
        ]


# Inputs:
#  - calibrated_measurement_set: path to the calibrated measurement set
#  - parameter_file_path: path to the parameter file
#  - input_h5: path to the input h5 file
# Outputs:
#    None
class ApplyCalibrationStep(PipelineStep):
    def build_command(
        self, calibrated_measurement_set: str, parameter_file_path: str, input_h5: str
    ) -> List[str]:
        return [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_measurement_set}",
            f"apply.parmdb={input_h5}",
        ]


# Inputs:
#  - calibrated_measurement_set: path to the calibrated measurement set
#
# Outputs:
#  - output_dir: path to the output directory where the .fits files will be saved
class ImagingStep(PipelineStep):
    def build_command(
        self, calibrated_measurement_set: str, output_dir: str
    ) -> List[str]:
        return [
            "wsclean",
            "-size",
            "1024",
            "1024",
            "-pol",
            "I",
            "-scale",
            "5arcmin",
            "-niter",
            "100000",
            "-gain",
            "0.1",
            "-mgain",
            "0.6",
            "-auto-mask",
            "5",
            "-local-rms",
            "-multiscale",
            "-no-update-model-required",
            "-make-psf",
            "-auto-threshold",
            "3",
            "-weight",
            "briggs",
            "0",
            "-data-column",
            "CORRECTED_DATA",
            "-nmiter",
            "0",
            "-name",
            output_dir,
            calibrated_measurement_set,
        ]


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
    ms = "/home/ayman/Downloads/entire_ms/SB205.MS"  # Can be the local or S3 Path
    calibrated_ms = "/home/ayman/Downloads/pipeline/SB205.ms"  # Local path of where the calibrated ms is being modified
    upload_calibrated_ms = (
        "/home/ayman/Downloads/pipeline/SB205.ms"  # Can be the local or S3 Path
    )
    h5 = "/home/ayman/Downloads/pipeline/cal_out/output.h5"  # Local path of the h5 file
    image_output_path = "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"  # Local path of the image file
    # Points to where the parameter files are located
    parameters_write_path = "/home/ayman/Downloads/pipeline/parameters"
    sourcedb_directory = f"{parameters_write_path}/cal/STEP2A-apparent.sourcedb"  # Local or S3 path of the sourcedb directory

    parameters = {
        "parameters": {
            "RebinningStep": {
                "measurement_set": ms,
                "parameter_file_path": PurePosixPath(
                    f"{parameters_write_path}/rebinning/STEP1-flagrebin.parset"
                ),
                "lua_file_dir": PurePosixPath("/home/ayman/Downloads/pipeline/parameters/rebinning/STEP1-NenuFAR64C1S.lua"),
                "write_path": PurePosixPath(calibrated_ms),
            },
            "CalibrationStep": {
                "calibrated_measurement_set": calibrated_ms,
                "parameter_file_path": PurePosixPath(
                    f"{parameters_write_path}/cal/STEP2A-calibration.parset"
                ),
                "output_h5": PurePosixPath(h5),
                "sourcedb_directory": sourcedb_directory,
            },
            "SubstractionStep": {
                "calibrated_measurement_set": PurePosixPath(calibrated_ms),
                "parameter_file_path": PurePosixPath(
                    f"{parameters_write_path}/sub/STEP2B-subtract.parset"
                ),
                "input_h5": PurePosixPath(h5),
                "sourcedb_directory": sourcedb_directory,
            },
            "ApplyCalibrationStep": {
                "calibrated_measurement_set": calibrated_ms,
                "parameter_file_path": f"{parameters_write_path}/apply/STEP2C-applycal.parset",
                "input_h5": h5,
            },
            "ImagingStep": {
                "calibrated_measurement_set": calibrated_ms,
                "output_dir": "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024",
            },
        }
    }

    # Run pipeline with parameters
    pipeline = Pipeline(
        parameters=parameters,
        executor=LithopsExecutor(),
        datasource=LithopsDataSource(),
    )
    pipeline.run()
