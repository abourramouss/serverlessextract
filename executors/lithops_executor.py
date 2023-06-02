import lithops
from .executor import Executor
from datasource import DataSource
from typing import List
class LithopsExecutor(Executor):
    def __init__(self):
        self.executor = lithops.FunctionExecutor()

    def execute(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        futures = self.executor.map(step.run, iterdata, extra_args=extra_args, extra_env=extra_env)
        results = self.executor.get_result(futures)
        return results
    
    def execute_call_async(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        futures = self.executor.call_async(step.run, (iterdata, *extra_args), extra_env=extra_env)
        results = self.executor.get_result(futures)
        return results
    
    