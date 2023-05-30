import subprocess
import os
from .step import Step

class ImagingStep(Step):
    def __init__(self, input_files, output_name, params=None):
        self.input_files = input_files
        self.output_name = output_name
        if params is None:
            self.params = [
                "-size", "1024", "1024",
                "-pol", "I",
                "-scale", "5arcmin",
                "-niter", "100000",
                "-gain", "0.1",
                "-mgain", "0.6",
                "-auto-mask", "5",
                "-local-rms",
                "-multiscale",
                "-no-update-model-required",
                "-make-psf",
                "-auto-threshold", "3",
                "-weight", "briggs", "0",
                "-data-column", "CORRECTED_DATA",
                "-nmiter", "0",
                "-name", self.output_name
            ]
        else:
            self.params = params

    def run(self):
        cmd = ["wsclean"] + self.params + self.input_files
        subprocess.run(cmd)

    def get_data(self):
        # Imaging step doesn't return any data that needs to be passed to another step.
        # So, return None or an empty list.
        return None
    
    