import subprocess
from .step import Step
from datasource import LithopsDataSource

class RebinningStep(Step):
    def __init__(self,parameter_file, lua_file):
        self.parameter_file = parameter_file
        self.lua_file = lua_file
        self.datasource = LithopsDataSource()

    def run(self, mesurement_set, output_file):
        #Download the decompressed mesurement set from object storage
        self.datasource.download("lithops-bucket-1", mesurement_set, output_file)

        # Run the bash command for each of input and output files
        
        cmd = [
            "DP3",
            f"PARAMETERS/{self.parameter_file}",
            f"msin={mesurement_set}",
            f"msout={output_file}"
        ]
        subprocess.run(cmd)
