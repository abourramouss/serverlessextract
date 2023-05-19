import lithops
from .executor import Executor
from datasource import DataSource
from typing import List
class LithopsExecutor(Executor):
    def __init__(self):
        self.executor = lithops.FunctionExecutor()

    def execute(self, step, mesurement_set_iterdata: List[str], bucket_name: str, output_dir: str):
    
        extra_args = [bucket_name, output_dir]
        futures = self.executor.map(step.run, mesurement_set_iterdata, extra_args=extra_args)
        results = self.executor.get_result(futures)
        return results