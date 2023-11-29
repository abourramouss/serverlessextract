"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from util import Profiler
from collections import namedtuple
import json

ProfilerKey = namedtuple(
    "ProfilerKey", ["step_name", "runtime_size", "chunk_size", "iteration"]
)


class ProfilerCollection:
    def __init__(self):
        self.profilers = {}

    def add_profilers(self, step_name, runtime_size, chunk_size, iteration, profiler):
        # If it already exists, update it, otherwise add it
        step = self.profilers.setdefault(step_name, {})
        runtime = step.setdefault(runtime_size, {})
        chunk = runtime.setdefault(chunk_size, {})
        chunk[iteration] = profiler

    def get_profilers(self, step_name, runtime_size, chunk_size, iteration):
        key = ProfilerKey(step_name, runtime_size, chunk_size, iteration)
        return self.profilers[key]

    def to_dict(self):
        return self.profilers

    def to_json(self):
        return json.dumps(self.to_dict())
