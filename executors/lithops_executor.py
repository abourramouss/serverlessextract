import lithops
from .executor import Executor
from datasource import DataSource
from typing import List
from steps.step import Step
import datetime


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
            stats = {}
            for step in steps:
                result_dict = step.run(data, *extra_args)
                result = result_dict['result']
                stat = result_dict['stats']

                data = result
                stats[step.__class__.__name__] = stat
            return {'result': data, 'stats': stats}

        futures = self.executor.map(
            execute_step, iterdata, extra_env=extra_env)
        results_and_stats = self.executor.get_result(fs=futures)
        # Assume futures is a list of the future objects
        futures_stats = [{'future': f, 'stats': f.stats} for f in futures]

        # Sort the futures_stats list based on 'worker_start_tstamp'
        futures_stats.sort(key=lambda x: x['stats']['worker_start_tstamp'])

        # The start time of the first worker
        first_worker_start_time = futures_stats[0]['stats']['worker_start_tstamp']

        # Calculate relative start times and update each dictionary in futures_stats
        for worker_stat in futures_stats:
            worker_start_time = worker_stat['stats']['worker_start_tstamp']
            relative_start_time = worker_start_time - first_worker_start_time
            worker_stat['relative_start_time'] = relative_start_time

            print(
                f"The relative start time for this worker is {relative_start_time} seconds.")

        return results_and_stats

    def execute_call_async(self, step, iterdata: List[str], extra_args: List[str], extra_env: dict):
        futures = self.executor.call_async(
            step.run, (iterdata, *extra_args), extra_env=extra_env)
        results = self.executor.get_result(futures)
        return results
