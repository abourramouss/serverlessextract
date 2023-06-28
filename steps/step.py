import os
import subprocess
from abc import ABC, abstractmethod
from datasource import LithopsDataSource
from utils import timeit_execution


class Step(ABC):

    @abstractmethod
    def run(self, measurement_set: str, bucket_name: str, output_dir: str):
        pass

    @timeit_execution
    def execute_command(self, cmd, capture=False):
        out = subprocess.run(cmd, capture_output=capture, text=capture)
        return out

    def get_size(self, start_path='.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        total_size_mb = total_size / (1024 * 1024)
        return total_size_mb
