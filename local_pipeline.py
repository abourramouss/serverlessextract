# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
from abc import ABC, abstractmethod
import os
import subprocess as sp
import psutil
import time
import matplotlib.pyplot as plt
from typing import List, Union, Optional
import shutil
import lithops
from lithops import Storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import S3Path
from pathlib import PurePosixPath


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
    def execute_step(self, step: PipelineStep, parameters: dict) -> None:
        pass

    @abstractmethod
    def execute_steps(self, steps: List[PipelineStep]) -> None:
        pass


class LocalExecutor(Executor):
    def execute_step(self, step: PipelineStep, parameters: Union[dict, list]) -> None:
        step(parameters[step.__class__.__name__])

    def execute_steps(
        self,
        steps: Union[List[PipelineStep], PipelineStep],
        parameters: Union[dict, list],
    ) -> None:
        if isinstance(steps, PipelineStep):
            self.execute_step(steps, parameters)
        elif isinstance(steps, list):
            for step in steps:
                self.execute_step(step, parameters)


class LithopsExecutor(Executor):
    def __init__(self):
        self.executor = lithops.FunctionExecutor()

    def execute_step(self, step: PipelineStep, parameters: Union[dict, list]) -> None:
        step(parameters[step.__class__.__name__])

    def execute_steps(
        self,
        steps: Union[List[PipelineStep], PipelineStep],
        parameters: Union[dict, list],
    ) -> None:
        def _execute_steps(
            self,
            steps: List[PipelineStep],
            parameters: Union[dict, list],
        ) -> None:
            if isinstance(steps, PipelineStep):
                self.execute_step(steps, parameters)
            elif isinstance(steps, list):
                for step in steps:
                    self.execute_step(step, parameters)

        futures = self.executor.map(_execute_steps, steps)
        self.executor.get_result(fs=futures)


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    @abstractmethod
    def download_file(self, read_path: str, write_path: str) -> None:
        pass

    @abstractmethod
    def download_ms(self, read_path: str, write_path: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, read_path: str, write_path: str) -> None:
        pass

    @abstractmethod
    def upload_ms(self, read_path: str, write_path: str) -> None:
        pass

    def remove_cached(self):
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


class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()

    def download_file(self, read_path: str, write_path: str) -> None:
        pass

    def download_ms(self, read_path: str, write_path: str) -> None:
        pass

    def upload_file(self, read_path: str, write_path: str) -> None:
        pass

    def upload_ms(self, read_path: str, write_path: str) -> None:
        pass


class LocalDataSource(DataSource):
    def download_file(self, read_path: str, write_path: str) -> None:
        pass

    def download_ms(self, read_path: str, write_path: str) -> None:
        pass

    def upload_file(self, read_path: str, write_path: str) -> None:
        pass

    def upload_ms(self, read_path: str, write_path: str) -> None:
        pass


# TODO: Enable ingestion of chunks or entire ms, rebinning measurement_set could potentially be a list of ms
# TODO: Refactor rebinning step in Lithops version, lua_file_path is not used,
# it's directly loaded from the parameter_file_path should be checked if it exists or removed
# TODO: Enable dynamic loading/linking of the parameter files, this means creating them on runtime,
# or downloading-modifying them also on runtime.
# TODO: Check input parameters for the Pipeline class, not all of them are needed


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
        self, parameters: dict, executor: Executor, datasource: DataSource
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

    def _prepare_rebinning(self):
        self._datasource.remove_cached()

        self._datasource.download_ms(
            self.parameters[RebinningStep.__name__]["measurement_set"],
            self.parameters[RebinningStep.__name__]["write_path"],
        )

        self._datasource.download_file(
            os.path.basename(
                self.parameters[RebinningStep.__name__]["parameter_file_path"]
            ),
            self.parameters[RebinningStep.__name__]["parameter_file_path"],
            "rebinning.lua",
        )

    def _prepare_imaging(self):
        self._datasource.download_ms(
            self.parameters[ImagingStep.__name__]["calibrated_measurement_set"],
            self.parameters[ImagingStep.__name__]["output_dir"],
        )

    def _finish_rebinning(self):
        self._datasource.upload_ms(
            self.parameters[ApplyCalibrationStep.__name__]["write_path"]
        )

    def _execute_pipeline(self, steps: List[PipelineStep], parameters: dict):
        self._executor.execute_steps(steps, parameters)

    def run_rebinning_calibration(self):
        self._prepare_rebinning()
        self._execute_pipeline(self.steps[:-1], self.parameters)
        self._finish_rebinning()

    def run_imaging(self):
        self._prepare_imaging()
        self._execute_pipeline(self.steps[-1], self.parameters)

    # Depending on the executor and datasource, we should prepare the parameters for execution i.e download ms and parameter files

    def run(self):
        self.run_rebinning_calibration()
        self.run_imaging()


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
# Outputs:
#   - write_path: creates a new measurement set in the write path
class RebinningStep(PipelineStep):
    def build_command(
        self, measurement_set: str, parameter_file_path: str, write_path: str
    ) -> List[str]:
        return [
            "DP3",
            parameter_file_path,
            f"msin={measurement_set}",
            f"msout={write_path}",
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

    # S3 Paths:
    # ms = "s3://aymanb-serverless-genomics/extract-data/partitions_60/SB205/SB205.MS"
    # parameters = s3://aymanb-serverless-genomics/extract-data/parameters
    # calibrated_ms = "s3://aymanb-serverless-genomics/pipeline/SB205.ms"
    # image_output_path = "s3://aymanb-serverless-genomics/pipeline/OUTPUT/Cygloop-205-210-b0-1024"

    ms = "/home/ayman/Downloads/entire_ms/SB205.MS"
    calibrated_ms = "/home/ayman/Downloads/pipeline/SB205.ms"
    h5 = "/home/ayman/Downloads/pipeline/cal_out/output.h5"
    image_output_path = "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"

    parameters = {
        "RebinningStep": {
            "measurement_set": ms,
            "parameter_file_path": "/home/ayman/Downloads/pipeline/parameters/rebinning/STEP1-flagrebin.parset",
            "write_path": calibrated_ms,
        },
        "CalibrationStep": {
            "calibrated_measurement_set": calibrated_ms,
            "parameter_file_path": "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-calibration.parset",
            "output_h5": h5,
            "sourcedb_directory": "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb",
        },
        "SubstractionStep": {
            "calibrated_measurement_set": calibrated_ms,
            "parameter_file_path": "/home/ayman/Downloads/pipeline/parameters/sub/STEP2B-subtract.parset",
            "input_h5": h5,
            "sourcedb_directory": "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb",
        },
        "ApplyCalibrationStep": {
            "calibrated_measurement_set": calibrated_ms,
            "parameter_file_path": "/home/ayman/Downloads/pipeline/parameters/apply/STEP2C-applycal.parset",
            "input_h5": h5,
        },
        "ImagingStep": {
            "calibrated_measurement_set": calibrated_ms,
            "output_dir": "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024",
        },
    }

    # Run pipeline with parameters
    pipeline = Pipeline(
        parameters=parameters,
        executor=LithopsExecutor(),
        datasource=LithopsDataSource(),
    )
    pipeline.run()
