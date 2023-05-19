import subprocess
from .step import Step
from datasource import LithopsDataSource
from datasource import DataSource

class RebinningStep(Step):
    def __init__(self,parameter_file, lua_file):
        self.parameter_file = parameter_file
        self.lua_file = lua_file

    def run(self, mesurement_set: str, bucket_name: str, output_dir: str) -> None:
        self.datasource = LithopsDataSource()
        #Download the decompressed mesurement set from object storage
        self.datasource.download(bucket_name, mesurement_set, output_dir)

        cmd = [
            "DP3",
            f"PARAMETERS/{self.parameter_file}",
            f"msin={mesurement_set}",
            f"msout={output_dir}"
        ]
        subprocess.run(cmd)
