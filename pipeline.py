from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep

class Pipeline:
    def __init__(self, mesurement_sets: List[str], steps: List[Step], executor: Executor):
        self.steps = steps
        self.executor = executor
        self.mesurement_sets = mesurement_sets

    def run(self):
        for step in self.steps:
            self.executor.execute(step, self.mesurement_sets)

if "__main__" == __name__:
    steps = [RebinningStep('parameters.txt', 'rebinning.lua')]
    executor = LithopsExecutor()
    mesurement_sets = []
    pipeline = Pipeline(mesurement_sets, steps, executor)
    pipeline.run()