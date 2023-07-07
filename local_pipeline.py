# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
import os
import subprocess as sp


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
        out = sp.run(cmd, capture_output=True, text=True)
        print(out.stdout)
        print(out.stderr)

        return write_path


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
        out = sp.run(cmd, capture_output=True, text=True)
        print(out.stdout)
        print(out.stderr)


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
        out = sp.run(cmd, capture_output=True, text=True)
        print(out.stdout)
        print(out.stderr)


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
        out = sp.run(cmd, capture_output=True, text=True)

        print(out.stdout)
        print(out.stderr)


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
        out = sp.run(cmd, capture_output=True, text=True)
        print(out.stdout)
        print(out.stderr)


if "__main__" == __name__:
    ms = "/home/ayman/Downloads/entire_ms/SB205.MS"
    write_path = "/home/ayman/Downloads/pipeline/SB205.ms"
    flagrebinparset = (
        "/home/ayman/Downloads/pipeline/parameters/rebinning/STEP1-flagrebin.parset"
    )

    rebinning_step = RebinningStep()
    cal_ms = rebinning_step(ms, flagrebinparset, write_path)

    h5_path = "/home/ayman/Downloads/pipeline/cal_out/output.h5"
    cal_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-calibration.parset"
    )
    source_db_path = (
        "/home/ayman/Downloads/pipeline/parameters/cal/STEP2A-apparent.sourcedb"
    )

    calibration_step = CalibrationStep()
    calibration_step(
        calibrated_measurement_set=cal_ms,
        parameter_file_path=cal_parameter_path,
        output_h5=h5_path,
        sourcedb_directory=source_db_path,
    )
    sub_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/sub/STEP2B-subtract.parset"
    )
    substraction_step = SubstractionStep()

    substraction_step(
        calibrated_mesurement_set=cal_ms,
        parameter_file_path=sub_parameter_path,
        input_h5=h5_path,
        sourcedb_directory=source_db_path,
    )
    apply_parameter_path = (
        "/home/ayman/Downloads/pipeline/parameters/apply/STEP2C-applycal.parset"
    )
    apply_calibration_step = ApplyCalibrationStep()
    apply_calibration_step(
        parameter_file_path=apply_parameter_path,
        calibrated_mesurement_set=cal_ms,
        input_h5=h5_path,
    )

    image_output = "/home/ayman/Downloads/pipeline/OUTPUT/Cygloop-205-210-b0-1024"
    imaging_step = ImagingStep()
    imaging_step(calibrated_mesurement_set=cal_ms, output_dir=image_output)
