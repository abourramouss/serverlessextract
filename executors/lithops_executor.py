import lithops
from .executor import Executor

class LithopsExecutor(Executor):
    def __init__(self):
        self.executor = lithops.FunctionExecutor()

    def execute(self, step, data):
        futures = self.executor.map(step.run, data)
        results = self.executor.get_result(futures)
        return results