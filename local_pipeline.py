# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
import os
import subprocess as sp
import psutil
import shutil
import time
from typing import List, Tuple
import matplotlib.pyplot as plt


def execute_command(cmd: List[str]) -> List[List[float]]:
    stats = []
    num_cores = psutil.cpu_count(logical=True)
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


import os


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


class LocalPipeline:
    def __init__(self, measurement_set: str, output_path: str) -> None:
        self.self_current_ms_path = measurement_set
        self.output_path = output_path


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
# Outputs:
#   - write_path: creates a new measurement set in the write path
class RebinningStep:
    # TODO: Refactor rebinning step in Lithops version, lua_file_path is not used,
    # it's directly loaded from the parameter_file_path should be checked if it exists or removed
    # TODO: Enable dynamic loading/linking of the parameter files, this means creating them on runtime,
    # or downloading-modifying them also on runtime.
    def __call__(
        self, measurement_set: str, parameter_file_path: str, write_path: str
    ) -> str:
        cmd = [
            "DP3",
            parameter_file_path,
            f"msin={measurement_set}",
            f"msout={write_path}",
        ]

        print("Rebinning step")

        stats = execute_command(cmd)

        return write_path, stats


# Inputs:
#   - measurement_set: path to the measurement set
#   - parameter_file_path: path to the parameter file
#   - sourcedb_directory: path to the sourcedb directory
# Outputs:
#   - output_h5: creates new h5 file in the output_h5 path
class CalibrationStep:
    def __call__(
        self,
        calibrated_measurement_set: str,
        parameter_file_path: str,
        output_h5: str,
        sourcedb_directory: str,
    ):
        cmd = [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_measurement_set}",
            f"cal.h5parm={output_h5}",
            f"cal.sourcedb={sourcedb_directory}",
        ]

        # We have to create the output file beforehand since DP3 doesn't do it
        print("Calibration step")
        stats = execute_command(cmd)
        return stats


# Inputs:
#   - calibrated_measurement_set: path to the calibrated measurement set
#   - parameter_file_path: path to the parameter file
#   - sourcedb_directory: path to the sourcedb directory
# Outputs:
#   - output_h5: creates new h5 file in the output_h5 path
class SubstractionStep:
    def __call__(
        self,
        parameter_file_path: str,
        calibrated_mesurement_set: str,
        input_h5: str,
        sourcedb_directory: str,
    ):
        cmd = [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_mesurement_set}",
            f"sub.applycal.parmdb={input_h5}",
            f"sub.sourcedb={sourcedb_directory}",
        ]

        print("Substraction step")
        stats = execute_command(cmd)
        return stats


# Inputs:
#  - calibrated_measurement_set: path to the calibrated measurement set
#  - parameter_file_path: path to the parameter file
#  - input_h5: path to the input h5 file
# Outputs:
#    None
class ApplyCalibrationStep:
    def __call__(
        self, parameter_file_path: str, calibrated_mesurement_set: str, input_h5: str
    ):
        cmd = [
            "DP3",
            parameter_file_path,
            f"msin={calibrated_mesurement_set}",
            f"apply.parmdb={input_h5}",
        ]
        print("Apply calibration step")
        stats = execute_command(cmd)
        return stats


# Inputs:
#  - calibrated_measurement_set: path to the calibrated measurement set
#
# Outputs:
#  - output_dir: path to the output directory where the .fits files will be saved
class ImagingStep:
    def __call__(self, calibrated_mesurement_set: str, output_dir: str):
        cmd = [
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
            calibrated_mesurement_set,
        ]
        print("Imaging step")
        stats = execute_command(cmd)
        return stats


if "__main__" == __name__:
    # Parameters for rebinning step
    ms = "/home/ayman/Downloads/entire_ms/SB205.MS"
    write_path = "/home/ayman/Downloads/pipeline/SB205.ms"
    flagrebinparset = (
        "/home/ayman/Downloads/pipeline/parameters/rebinning/STEP1-flagrebin.parset"
    )

    # Parameters for calibration step

    h5_path = "/home/ayman/Downloads/pipeline/cal_out/output.h5"
    cal_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-calibration.parset"
    )
    source_db_path = (
        "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb"
    )

    # Parameters for substraction step
    sub_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/sub/STEP2B-subtract.parset"
    )

    # Parameters for apply calibration step
    apply_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/apply/STEP2C-applycal.parset"
    )

    # Parameters for imaging step

    image_output = "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"

    # Check if there is any previous results from a previous execution

    if os.path.exists(write_path):
        shutil.rmtree(write_path)

    if os.path.isfile(h5_path):
        os.remove(h5_path)

    if os.path.exists(image_output):
        images = os.listdir(image_output)
        for image in images:
            os.remove(os.path.join(image_output, image))

    rebinning_step = RebinningStep()
    cal_ms, stats = rebinning_step(
        measurement_set=ms, parameter_file_path=flagrebinparset, write_path=write_path
    )

    plot_stats(stats, "stats", rebinning_step.__class__.__name__)
    calibration_step = CalibrationStep()
    stats = calibration_step(
        calibrated_measurement_set=cal_ms,
        parameter_file_path=cal_parameter_path,
        output_h5=h5_path,
        sourcedb_directory=source_db_path,
    )
    plot_stats(stats, "stats", calibration_step.__class__.__name__)

    substraction_step = SubstractionStep()

    stats = substraction_step(
        calibrated_mesurement_set=cal_ms,
        parameter_file_path=sub_parameter_path,
        input_h5=h5_path,
        sourcedb_directory=source_db_path,
    )

    plot_stats(stats, "stats", substraction_step.__class__.__name__)

    apply_calibration_step = ApplyCalibrationStep()
    stats = apply_calibration_step(
        parameter_file_path=apply_parameter_path,
        calibrated_mesurement_set=cal_ms,
        input_h5=h5_path,
    )
    plot_stats(stats, "stats", apply_calibration_step.__class__.__name__)

    imaging_step = ImagingStep()
    stats = imaging_step(calibrated_mesurement_set=cal_ms, output_dir=image_output)
    plot_stats(stats, "stats", imaging_step.__class__.__name__)
