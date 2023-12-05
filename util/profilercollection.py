"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from util import Profiler
import json
from dataclasses import dataclass
from typing import List
import time


@dataclass
class StepProfiler:
    step_name: str
    memory: int
    chunk_size: int
    profilers: List[Profiler]  # Ensure this is a list

    def to_dict(self):
        return {
            "step_name": self.step_name,
            "memory": self.memory,
            "chunk_size": self.chunk_size,
            "profilers": [profiler.to_dict() for profiler in self.profilers],
        }

    @staticmethod
    def from_dict(data):
        return StepProfiler(
            step_name=data["step_name"],
            memory=data["memory"],
            chunk_size=data["chunk_size"],
            profilers=[
                Profiler.from_dict(profiler_data) for profiler_data in data["profilers"]
            ],
        )


class ProfilerCollection:
    def __init__(self):
        self.step_profilers = {}

    def add_step_profiler(self, step_name, memory, chunk_size, profilers):
        if not isinstance(profilers, list):
            raise TypeError("profilers should be a list of Profiler instances")
        execution_id = f"{step_name}_{int(time.time())}"
        step_profiler = StepProfiler(
            step_name=step_name,
            memory=memory,
            chunk_size=chunk_size,
            profilers=profilers,
        )
        self.step_profilers[execution_id] = step_profiler

    def get_step_profiler(self, execution_id):
        return self.step_profilers.get(execution_id)

    def to_dict(self):
        return {
            execution_id: step_profiler.to_dict()
            for execution_id, step_profiler in self.step_profilers.items()
        }
