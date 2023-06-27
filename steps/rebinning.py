import subprocess
from .step import Step
from datasource import LithopsDataSource
from datasource import DataSource
import os
from utils import delete_all_in_cwd


class RebinningStep(Step):
    def __init__(self, parameter_file, lua_file):
        self.parameter_file = parameter_file
        self.lua_file = lua_file

    def run(self, mesurement_set: str, bucket_name: str, output_dir: str):

        self.datasource = LithopsDataSource()
        os.chdir(output_dir)
        os.makedirs('DATAREB', exist_ok=True)

        mesurement_set_name = mesurement_set.split('/')[-1]
        output_path = f"/tmp/DATAREB/cal_{mesurement_set_name}"

        # First download the measurement set and the parameters needed
        download_timing_1 = self.datasource.download(
            bucket_name, mesurement_set, output_dir)
        download_timing_2 = self.datasource.download(
            bucket_name, f"extract-data/parameters", output_dir)

        print("Download timing")
        print(download_timing_1)
        print(download_timing_2)
        cmd = [
            "DP3",
            self.parameter_file,
            f"msin={mesurement_set}",
            f"msout={output_path}",
        ]

        timing = self.execute_command(cmd, capture=False)

        return {'result': output_path, 'timing': {'execution': timing, 'io': sum(download_timing_1[1] + download_timing_2[1])}}
