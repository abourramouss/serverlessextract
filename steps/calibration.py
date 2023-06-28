import subprocess
from .step import Step
from datasource import LithopsDataSource
import os
import time


class CalibrationStep(Step):
    def __init__(self, calibration_file, skymodel_file, sourcedb_file):
        self.skymodel_file = skymodel_file
        self.sourcedb_file = sourcedb_file
        self.calibration_file = calibration_file

    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str):

        self.datasource = LithopsDataSource()
        os.chdir(output_dir)

        print("Calibration step")
        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"

        curr_dir = os.getcwd()
        os.chdir(f'/tmp/DATAREB')
        for file in os.listdir():
            print(file)

        os.chdir(curr_dir)

        cmd = [
            "DP3",
            self.calibration_file,
            f"msin={calibrated_mesurement_set}",
            f"cal.h5parm=/tmp/DATAREB/{output_h5}",
            f"cal.sourcedb={self.sourcedb_file}"
        ]

        timing = self.execute_command(cmd, capture=False)
        return {'result': calibrated_mesurement_set, 'stats': {'execution': timing}}


class SubtractionStep(Step):
    def __init__(self, calibration_file, sourcedb_file):
        self.calibration_file = calibration_file
        self.source_db = sourcedb_file

    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str):

        self.datasource = LithopsDataSource()
        os.chdir(output_dir)

        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"

        cmd = [
            "DP3",
            self.calibration_file,
            f"msin={calibrated_mesurement_set}",
            f"sub.applycal.parmdb=/tmp/DATAREB/{output_h5}",
            f"sub.sourcedb={self.source_db}",
        ]

        timing = self.execute_command(cmd, capture=False)
        return {'result': calibrated_mesurement_set, 'stats': {'execution': timing}}


class ApplyCalibrationStep(Step):
    def __init__(self, calibration_file):
        self.calibration_file = calibration_file

    def run(self, calibrated_mesurement_set: str, bucket_name: str, output_dir: str):

        self.datasource = LithopsDataSource()
        os.chdir(output_dir)

        calibrated_name = calibrated_mesurement_set.split('/')[-1]
        calibrated_name = calibrated_name.split('.')[0]
        output_h5 = f"{calibrated_name}.h5"

        cmd = [
            "DP3",
            self.calibration_file,
            f"msin={calibrated_mesurement_set}",
            f"apply.parmdb=DATAREB/{output_h5}"
        ]

        print(cmd)

        time = self.execute_command(cmd, capture=False)
        upload_timing = self.datasource.upload(
            bucket_name, 'extract-data/step2c_out', calibrated_mesurement_set)

        return {'result': f'extract-data/step2c_out/{calibrated_name}.ms', 'stats': {'execution': time, 'upload_time': upload_timing, 'upload_size': self.get_size(calibrated_mesurement_set)}}
