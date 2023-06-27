import lithops
from .executor import Executor
from datasource import DataSource
from typing import List
from steps.step import Step


class LithopsExecutor(Executor):
    def __init__(self):
        self.executor = lithops.FunctionExecutor()

    def execute(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        futures = self.executor.map(
            step.run, iterdata, extra_args=extra_args, extra_env=extra_env)
        results = self.executor.get_result(futures)
        return results

    def execute_steps(self, steps: List[Step], iterdata: List[str], extra_args: List[str], extra_env: dict):
        def execute_step(data):
            timings = {}
            for step in steps:
                result_dict = step.run(data, *extra_args)
                result = result_dict['result']
                timing = result_dict['timing']

                data = result
                timings[step.__class__.__name__] = timing
            return {'result': data, 'timing': timings}

        futures = self.executor.map(
            execute_step, iterdata, extra_env=extra_env)
        results_and_timings = self.executor.get_result(
            futures)  # this now holds results and timings
        return results_and_timings

    def execute_call_async(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        futures = self.executor.call_async(
            step.run, (iterdata, *extra_args), extra_env=extra_env)
        results = self.executor.get_result(futures)
        return results
