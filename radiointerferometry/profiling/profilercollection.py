import json
import os
from dataclasses import dataclass, field
from typing import List, Optional
from ..profiling import Profiler


@dataclass
class CompletedStep:
    chunk_size: int
    step_name: str
    memory: int
    cpus_per_worker: int
    number_workers: int
    start_time: float
    end_time: float
    profilers: List[Profiler]
    step_id: str
    environment: Optional[str] = field(default=None)
    instance_type: Optional[str] = field(default=None)

    def to_dict(self):
        return {
            "step_name": self.step_name,
            "step_id": self.step_id,
            "chunk_size": self.chunk_size,
            "memory": self.memory,
            "cpus_per_worker": self.cpus_per_worker,
            "number_workers": self.number_workers,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "environment": self.environment,
            "instance_type": self.instance_type,
            "profilers": [profiler.to_dict() for profiler in self.profilers],
        }

    @classmethod
    def from_dict(cls, data):
        profilers = [Profiler.from_dict(p) for p in data["profilers"]]
        return cls(
            chunk_size=data["chunk_size"],
            memory=data["memory"],
            cpus_per_worker=data["cpus_per_worker"],
            number_workers=data["number_workers"],
            start_time=data["start_time"],
            end_time=data["end_time"],
            environment=data.get("environment"),
            instance_type=data.get("instance_type"),
            profilers=profilers,
            step_id=data["step_id"],
            step_name=data["step_name"],
        )


class CompletedWorkflow:
    def __init__(self):
        self.completed_steps = []

    def __iter__(self):
        return iter(self.completed_steps)

    def add_completed_step(
        self,
        completed_step: CompletedStep,
    ):
        self.completed_steps.append(completed_step)

    def to_dict(self):
        return {
            "completed_steps": [step.to_dict() for step in self.completed_steps],
        }

    @classmethod
    def from_dict(cls, data):
        workflow = cls(data)
        for step_data in data["completed_steps"]:
            workflow.add_completed_step(**step_data)
        return workflow


class CompletedWorkflowsCollection:
    def __init__(self):
        self.workflows = {}

    def __getitem__(self, workflow_name):
        return self.workflows[workflow_name]

    def __len__(self):
        return sum(
            len(workflow.completed_steps) for workflow in self.workflows.values()
        )

    def __iter__(self):
        for workflow_name, workflow in self.workflows.items():
            for completed_step in workflow.completed_steps:
                yield (workflow_name, completed_step)

    def add_workflow_step(
        self,
        workflow_name,
        chunk_size,
        memory,
        cpus_per_worker,
        start_time,
        end_time,
        profilers,
        step_id,
        number_workers,
        instance_type=None,
        environment=None,
    ):
        if workflow_name not in self.workflows:
            self.workflows[workflow_name] = CompletedWorkflow(workflow_name)
        self.workflows[workflow_name].add_completed_step(
            chunk_size,
            memory,
            cpus_per_worker,
            number_workers,
            start_time,
            end_time,
            profilers,
            step_id,
            instance_type,
            environment,
        )

    def to_dict(self):
        return {
            workflow_name: workflow.to_dict()
            for workflow_name, workflow in self.workflows.items()
        }

    def save_to_file(self, file_path):
        with open(file_path, "w") as file:
            json.dump(self.to_dict(), file, indent=4)

    @classmethod
    def load_from_file(cls, file_path):
        workflow_collection = cls()
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as file:
                data = json.load(file)
            for workflow_name, workflow_data in data.items():
                workflow = CompletedWorkflow.from_dict(workflow_data)
                workflow_collection.workflows[workflow_name] = workflow
        return workflow_collection
