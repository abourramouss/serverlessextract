import subprocess
from .step import Step
from datasource import LithopsDataSource
from datasource import DataSource
import os
from util import delete_all_in_cwd
from lithops import Storage

class RebinningStep(Step):
    def __init__(self, parameter_file, lua_file):
        self.parameter_file = parameter_file
        self.lua_file = lua_file
    def run(self, measurement_set: str, bucket_name: str, output_dir: str):
        self.storage = Storage()

        os.chdir(output_dir)
        os.makedirs("DATAREB", exist_ok=True)

        measurement_set_name = measurement_set.split("/")[-1]
        output_path = f"/tmp/DATAREB/cal_{measurement_set_name}"

        # Download the measurement set
        chunk_files = self.storage.list_keys(bucket_name, prefix=measurement_set)
        for file in chunk_files:
            print(file)
            created = os.makedirs(os.path.dirname(output_path+"/"+file.split("/")[-1]), exist_ok=True)
            if created:
                self.storage.download_file(bucket_name, file, file_name=output_path+"/"+file.split("/")[-1])
            else:
                raise Exception("Failed to create directory")
       
        return {
            "result": 1,
            "stats": {
                "execution": 1,
                "download_time": 1 + 1,
                "download_size": self.get_size(measurement_set),
            },
        }
