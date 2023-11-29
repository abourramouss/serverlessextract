"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from util import Profiler
from collections import namedtuple

ProfilerKey = namedtuple(
    "ProfilerKey", ["step_name", "runtime_size", "chunk_size", "iteration"]
)


class ProfilerCollection:
    def __init__(self):
        self.profilers = {}

    def add_profilers(self, step_name, runtime_size, chunk_size, iteration, profiler):
        key = ProfilerKey(step_name, runtime_size, chunk_size, iteration)
        self.profilers[key] = profiler

    def get_profilers(self, step_name, runtime_size, chunk_size, iteration):
        key = ProfilerKey(step_name, runtime_size, chunk_size, iteration)
        return self.profilers[key]
