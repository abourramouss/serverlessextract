import subprocess
from .step import Step
from datasource import LithopsDataSource
from datasource import DataSource
import os
from utils import delete_all_in_cwd
import time


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
        self.datasource.download(bucket_name, mesurement_set, output_dir)
        self.datasource.download(
            bucket_name, f"extract-data/parameters", output_dir)

        cmd = [
            "DP3",
            self.parameter_file,
            f"msin={mesurement_set}",
            f"msout={output_path}",
        ]

        out = subprocess.run(cmd, capture_output=True, text=True)

        return output_path
