import subprocess
import os
from .step import Step
from datasource import LithopsDataSource
import time


class ImagingStep(Step):

    def __init__(self, output_name):
        self.output_name = output_name

    def run(self, input_files: list, bucket_name: str, output_dir: str):

        self.datasource = LithopsDataSource()

        os.chdir(output_dir)

        # Download operation (I/O)
        for calibrated_mesurement_set in input_files:
            self.datasource.download(
                bucket_name, calibrated_mesurement_set, output_dir)

        cmd = [
            "wsclean",
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
            "-name", os.path.join(output_dir, self.output_name)
        ]

        # Append all the input files to the command
        cmd.extend(input_files)

        print(cmd)
        image_dir = os.path.join(output_dir, self.output_name)
        img_dir = os.path.dirname(image_dir)
        os.makedirs(img_dir, exist_ok=True)

        out = subprocess.run(cmd, capture_output=True, text=True)

        self.datasource.upload(bucket_name, 'extract-data/step3_out', img_dir)
