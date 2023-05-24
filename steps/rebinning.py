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
    
    def run(self, mesurement_set: str, bucket_name: str, output_dir: str) -> None:
        self.datasource = LithopsDataSource()
        os.chdir(output_dir)
        mesurement_set_name = mesurement_set.split('/')[-1]
        #download the mesurement set
        self.datasource.download(bucket_name, mesurement_set, output_dir)
        #download the parameter folder for DP3
        self.datasource.download(bucket_name, self.parameter_file, output_dir)
        print(os.listdir('/tmp/extract-data/parameters'))

        
        
        os.makedirs('DATAREB', exist_ok=True)
        
        cmd = [
            "DP3",
            self.parameter_file,
            f"msin={mesurement_set}",
            f"msout=DATAREB/{mesurement_set_name}",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        print(result.stdout)
        print(result.stderr)

        #upload for testing purposes
        self.datasource.upload(bucket_name, 'extract-data/DATAREB', f'/tmp/DATAREB/{mesurement_set_name}')
        
        return f'extract-data/DATAREB/{mesurement_set_name}'