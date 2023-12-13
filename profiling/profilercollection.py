"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from profiling import Profiler
import json
from dataclasses import dataclass
from typing import List
import os


@dataclass
class StepProfiler:
    step_name: str
    memory: int
    chunk_size: int
    profilers: List[Profiler]

    def __repr__(self) -> str:
        return f"StepProfiler({self.step_name}, {self.memory}, {self.chunk_size}, {self.profilers})"

    def __iter__(self):
        for profiler in self.profilers:
            yield profiler

    def to_dict(self):
        return {
            "step_name": self.step_name,
            "memory": self.memory,
            "chunk_size": self.chunk_size,
            "profilers": [profiler.to_dict() for profiler in self.profilers],
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
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

    def __repr__(self) -> str:
        return f"ProfilerCollection({self.step_profilers})"

    def __iter__(self):
        for step_name, memory_chunk in self.step_profilers.items():
            for key, exec_list in memory_chunk.items():
                memory, chunk_size = key
                for profilers in exec_list:
                    yield StepProfiler(step_name, memory, chunk_size, profilers)

    def add_step_profiler(self, step_name, memory, chunk_size, profilers):
        key = (memory, chunk_size)
        if step_name not in self.step_profilers:
            self.step_profilers[step_name] = {}
        if key not in self.step_profilers[step_name]:
            self.step_profilers[step_name][key] = []

        # Append new profiler list to the existing list of executions
        self.step_profilers[step_name][key].append(profilers)
        print(f"Updated step_profilers: {self.step_profilers}")

    def get_step_profilers(self, step_name, memory, chunk_size):
        key = (memory, chunk_size)
        return self.step_profilers.get(step_name, {}).get(key, [])

    def to_dict(self):
        serialized_data = {
            step_name: {
                str(key): [
                    [profiler.to_dict() for profiler in profilers]
                    for profilers in exec_list
                ]
                for key, exec_list in memory_chunk.items()
            }
            for step_name, memory_chunk in self.step_profilers.items()
        }

        return serialized_data

    def save_to_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        with open(file_path, "w") as file:
            json.dump(self.to_dict(), file, indent=4)

    @classmethod
    def load_from_file(cls, file_path):
        existing_collection = cls()
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as file:
                data = json.load(file)

            for step_name, memory_chunk in data.items():
                for key_str, exec_list in memory_chunk.items():
                    memory, chunk_size = map(int, key_str.strip("()").split(", "))
                    if all(isinstance(el, list) for el in exec_list):
                        for profilers_data in exec_list:
                            profilers = [
                                Profiler.from_dict(profiler_data)
                                for profiler_data in profilers_data
                            ]
                            existing_collection.add_step_profiler(
                                step_name, memory, chunk_size, profilers
                            )
                    else:
                        profilers = [
                            Profiler.from_dict(profiler_data)
                            for profiler_data in exec_list
                        ]
                        existing_collection.add_step_profiler(
                            step_name, memory, chunk_size, profilers
                        )
        return existing_collection
