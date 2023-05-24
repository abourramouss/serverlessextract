import subprocess
from .step import Step
from datasource import LithopsDataSource
import os
class CalibrationStep(Step):
    def __init__(self, calibration_file, skymodel_file, sourcerd_file):
        self.skymodel_file = skymodel_file
        self.sourcerd_file = sourcerd_file
        self.calibration_file = calibration_file
        
    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str) -> None:
        
        self.datasource = LithopsDataSource()
        
        os.chdir(output_dir)
        #Download rebined mesurement set
        self.datasource.download(bucket_name, calibrated_mesurement_set, output_dir)
       
        #Download parameters folder
        self.datasource.download(bucket_name, self.skymodel_file, output_dir)
        
        cmd = [
                "DP3",
                self.calibration_file,
                f"msin={calibrated_mesurement_set}",
                f"sub.applycal.parmdb=output/{calibrated_mesurement_set}.h5",
                f"sub.sourcedb={self.skymodel_file}"
            ]
        
        
        
        out = subprocess.run(cmd,  capture_output=True, text=True)
        
        print(out.stdout)
        print(out.stderr)
    

    

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
