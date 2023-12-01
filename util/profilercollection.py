"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from util import Profiler
from collections import namedtuple
import json


class ProfilerCollection:
    def __init__(self):
        # Represents a dict of lists of profilers
        self.profilers = {}

    def add_profilers(
        self, step_name, runtime_size, chunk_size, iteration, profiler_list
    ):
        # If it already exists, update it, otherwise add it
        step = self.profilers.setdefault(step_name, {})
        runtime = step.setdefault(runtime_size, {})
        chunk = runtime.setdefault(chunk_size, {})
        chunk[iteration] = profiler_list

    def get_profilers(self, step_name, runtime_size, chunk_size, iteration):
        return self.profilers[step_name][runtime_size][chunk_size][iteration]

    def to_dict(self):
        return {
            step_name: {
                runtime_size: {
                    chunk_size: {
                        iteration: [profiler.to_dict() for profiler in profiler_list]
                        for iteration, profiler_list in chunk.items()
                    }
                    for chunk_size, chunk in runtime.items()
                }
                for runtime_size, runtime in step.items()
            }
            for step_name, step in self.profilers.items()
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        profiler_collection = cls()
        for step_name, step in data.items():
            for runtime_size, runtime in step.items():
                for chunk_size, chunk in runtime.items():
                    for iteration, profiler_list in chunk.items():
                        for profiler in profiler_list:
                            profiler_collection.add_profilers(
                                step_name,
                                runtime_size,
                                chunk_size,
                                iteration,
                                Profiler.from_dict(profiler),
                            )
        return profiler_collection
