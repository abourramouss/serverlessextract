from pipeline import PipelineStep


class Pipeline:
    def __init__(self):
        self.step_queue = []

    def append_step(self, step: PipelineStep):
        self.step_stack.append(step)
