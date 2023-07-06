from .executor import Executor
from datasource import DataSource
from typing import List
from steps.step import Step
import time


# A sequential local executor
class LocalExecutor(Executor):
    def __init__(self):
        self.executor = self.__class__.__name__

    def execute(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        results = step.run(iterdata, *extra_args)
        return results

    def execute_steps(self, steps: List[Step], iterdata: List[str], extra_args: List[str], extra_env: dict):
        results_and_stats = []

        def execute_step(data):
            start_time = time.time()  # record the start time
            stats = {}
            for step in steps:
                print(f"Executing step {step.__class__.__name__}")
                result_dict = step.run(data, *extra_args)
                result = result_dict["result"]
                stat = result_dict["stats"]

                data = result
                stats[step.__class__.__name__] = stat
            return {"result": data, "stats": stats, "start_time": start_time}

        results_and_stats = results_and_stats + execute_step(iterdata)

        return results_and_stats

    def execute_call_async(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        raise NotImplementedError
