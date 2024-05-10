import json
import os
from dataclasses import dataclass, field
from typing import List, Optional
from radiointerferometry.profiling import Profiler


@dataclass
class CompletedStep:
    step_name: str
    step_cost: float  # In dollars
    step_ingested_size: str  # In mb
    memory: int
    cpus_per_worker: int
    number_workers: int
    start_time: float
    end_time: float
    profilers: List[Profiler]
    step_id: str
    environment: Optional[str] = None
    instance_type: Optional[str] = None

    def to_dict(self):
        return {
            "step_name": self.step_name,
            "step_id": self.step_id,
            "step_cost": self.step_cost,
            "step_ingested_size": self.step_ingested_size,
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
            step_name=data["step_name"],
            step_cost=data["step_cost"],
            step_ingested_size=data["step_ingested_size"],
            memory=data["memory"],
            cpus_per_worker=data["cpus_per_worker"],
            number_workers=data["number_workers"],
            start_time=data["start_time"],
            end_time=data["end_time"],
            environment=data.get("environment"),
            instance_type=data.get("instance_type"),
            profilers=profilers,
            step_id=data["step_id"],
        )


class CompletedWorkflow:
    def __init__(self):
        self.completed_steps = []
        self.total_workflow_cost = None
        self.client_step_start = None
        self.client_step_end = None
        self.workflow_id = None

    def __iter__(self):
        return iter(self.completed_steps)

    def add_completed_step(self, completed_step: CompletedStep):
        self.completed_steps.append(completed_step)

    def to_dict(self):
        return {"completed_steps": [step.to_dict() for step in self.completed_steps]}

    @classmethod
    def from_dict(cls, data):
        workflow = cls()
        for step_data in data["completed_steps"]:
            step = CompletedStep.from_dict(step_data)
            workflow.add_completed_step(step)
        return workflow


class CompletedWorkflowsCollection:
    def __init__(self, file_path=None):
        self.completed_workflows = []
        if file_path and os.path.exists(file_path):
            self.load_from_file(file_path)

    def __getitem__(self, index):
        return self.completed_workflows[index]

    def __len__(self):
        return len(self.completed_workflows)

    def __iter__(self):
        return iter(self.completed_workflows)

    def add_completed_workflow(self, completed_workflow: CompletedWorkflow):
        self.completed_workflows.append(completed_workflow)

    def to_dict(self):
        return {
            str(index): workflow.to_dict()
            for index, workflow in enumerate(self.completed_workflows)
        }

    def save_to_file(self, file_path):
        with open(file_path, "w") as file:
            json.dump(self.to_dict(), file, indent=4)

    def load_from_file(self, file_path):
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as file:
                data = json.load(file)
            for index, workflow_data in data.items():
                workflow = CompletedWorkflow.from_dict(workflow_data)
                self.add_completed_workflow(workflow)
