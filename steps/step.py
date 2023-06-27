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
