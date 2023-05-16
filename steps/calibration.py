import subprocess
from .step import Step

class CalibrationStep(Step):
    def __init__(self, input_files, output_files, skymodel_file, calibration_file):
        self.input_files = input_files
        self.output_files = output_files
        self.skymodel_file = skymodel_file
        self.calibration_file = calibration_file

    def run(self):
        for input_file, output_file in zip(self.input_files, self.output_files):
            cmd = [
                "DP3",
                self.calibration_file,
                f"msin={input_file}",
                f"cal.h5parm={output_file}",
                f"cal.sourcedb={self.skymodel_file}"
            ]
            subprocess.run(cmd)

    def get_data(self):
        return list(zip(self.input_files, self.output_files))


class SubtractionStep(Step):
    def __init__(self, input_files, h5_files, skymodel_file, calibration_file):
        self.input_files = input_files
        self.h5_files = h5_files
        self.skymodel_file = skymodel_file
        self.calibration_file = calibration_file

    def run(self):
        for input_file, h5_file in zip(self.input_files, self.h5_files):
            cmd = [
                "DP3",
                self.calibration_file,
                f"msin={input_file}",
                f"sub.applycal.parmdb={h5_file}",
                f"sub.sourcedb={self.skymodel_file}"
            ]
            subprocess.run(cmd)

    def get_data(self):
        return list(zip(self.input_files, self.h5_files))


class ApplyCalibrationStep(Step):
    def __init__(self, input_files, h5_files, applycal_file):
        self.input_files = input_files
        self.h5_files = h5_files
        self.applycal_file = applycal_file

    def run(self):
        for input_file, h5_file in zip(self.input_files, self.h5_files):
            cmd = [
                "DP3",
                self.applycal_file,
                f"msin={input_file}",
                f"apply.parmdb={h5_file}"
            ]
            subprocess.run(cmd)

    def get_data(self):
        return list(zip(self.input_files, self.h5_files))
