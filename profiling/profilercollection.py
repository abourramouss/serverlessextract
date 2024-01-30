"""This class serves as a container for multiple profilers, since there are multiple profilers outputted by each step and iterations, it serves as a higher level abstraction to simplify the code"""

from profiling import Profiler
import json
from dataclasses import dataclass
from typing import List
import os
import uuid


@dataclass
class Job:
    job_id: str
    chunk_size: int
    memory: int
    number_workers: int
    profilers: List[Profiler]

    def __init__(
        self,
        chunk_size: int,
        memory: int,
        number_workers: int,
        start_time: float,
        end_time: float,
        profilers: List[Profiler],
    ):
        self.job_id = str(uuid.uuid4())
        self.chunk_size = chunk_size
        self.memory = memory
        self.number_workers = number_workers
        self.start_time = start_time
        self.end_time = end_time
        self.profilers = profilers

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "chunk_size": self.chunk_size,
            "memory": self.memory,
            "number_workers": self.number_workers,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "profilers": [profiler.to_dict() for profiler in self.profilers],
        }

    @classmethod
    def from_dict(cls, data):
        profilers = [Profiler.from_dict(p) for p in data["profilers"]]
        return cls(
            data["chunk_size"],
            data["memory"],
            data["start_time"],
            data["end_time"],
            profilers,
        )


class Step:
    def __init__(self, step_name: str):
        self.step_name = step_name
        self.jobs = []

    def add_job(
        self,
        chunk_size: int,
        memory: int,
        start_time: float,
        end_time: float,
        profilers: List[Profiler],
    ):
        job = Job(chunk_size, memory, len(profilers), start_time, end_time, profilers)
        self.jobs.append(job)

    def to_dict(self):
        return {
            "step_name": self.step_name,
            "jobs": [job.to_dict() for job in self.jobs],
        }

    @classmethod
    def from_dict(cls, data):
        step = cls(data["step_name"])
        for job_data in data["jobs"]:
            step.add_job(
                job_data["chunk_size"],
                job_data["memory"],
                job_data["start_time"],
                job_data["end_time"],
                [Profiler.from_dict(p) for p in job_data["profilers"]],
            )
        return step


class JobCollection:
    def __init__(self):
        self.steps = {}  # Dict[str, Step]

    def __len__(self):
        return sum(len(step.jobs) for step in self.steps.values())

    def __iter__(self):
        for step_name, step in self.steps.items():
            for job in step.jobs:
                yield (step_name, job)

    def get_profilers(self, step_name, memory, chunk_size):
        return [
            job.profilers
            for job in self.steps.get(step_name, []).jobs
            if job.memory == memory and job.chunk_size == chunk_size
        ]

    def add_step_profiler(
        self, step_name, memory, chunk_size, start_time, end_time, profilers
    ):
        if step_name not in self.steps:
            self.steps[step_name] = Step(step_name)
        self.steps[step_name].add_job(
            chunk_size, memory, start_time, end_time, profilers
        )

    def get_jobs_by_memory_and_chunk_size(self, memory, chunk_size):
        jobs = []
        for step in self.steps.values():
            for job in step.jobs:
                if job.memory == memory and job.chunk_size == chunk_size:
                    jobs.append(job)
        return jobs

    def to_dict(self):
        return {step_name: step.to_dict() for step_name, step in self.steps.items()}

    def save_to_file(self, file_path):
        with open(file_path, "w") as file:
            json.dump(self.to_dict(), file, indent=4)

    @classmethod
    def load_from_file(cls, file_path):
        job_collection = cls()
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as file:
                data = json.load(file)
            for step_name, step_data in data.items():
                step = Step.from_dict(step_data)
                job_collection.steps[step_name] = step
        return job_collection
