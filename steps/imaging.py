import subprocess
import os
from .step import Step
from datasource import LithopsDataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


class ImagingStep(Step):
    def __init__(self, output_name):
        self.output_name = output_name

    def run(self, input_files: list, bucket_name: str, output_dir: str):
        self.datasource = LithopsDataSource()

        os.chdir(output_dir)

        download_time = 0
        download_size = 0
        # sequential download operation (I/O)
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            download_futures = {
                executor.submit(
                    self.datasource.download,
                    bucket_name,
                    calibrated_mesurement_set,
                    output_dir,
                ): calibrated_mesurement_set
                for calibrated_mesurement_set in input_files
            }

        for future in as_completed(download_futures):
            calibrated_mesurement_set = download_futures[future]
            try:
                data = future.result()
                print(f"Finished downloading {calibrated_mesurement_set}")
            except Exception as exc:
                print(f"Generated an exception: {calibrated_mesurement_set} {exc}")

        download_size = sum(
            self.get_size(calibrated_mesurement_set)
            for calibrated_mesurement_set in input_files
        )

        print(f"Total size of downloaded files: {download_size / (1024 * 1024)} MB")

        cmd = [
            "wsclean",
            "-size",
            "1024",
            "1024",
            "-pol",
            "I",
            "-scale",
            "5arcmin",
            "-niter",
            "100000",
            "-gain",
            "0.1",
            "-mgain",
            "0.6",
            "-auto-mask",
            "5",
            "-local-rms",
            "-multiscale",
            "-no-update-model-required",
            "-make-psf",
            "-auto-threshold",
            "3",
            "-weight",
            "briggs",
            "0",
            "-data-column",
            "CORRECTED_DATA",
            "-nmiter",
            "0",
            "-name",
            os.path.join(output_dir, self.output_name),
        ]

        # Append all the input files to the command
        cmd.extend(input_files)

        print(cmd)
        image_dir = os.path.join(output_dir, self.output_name)
        img_dir = os.path.dirname(image_dir)
        os.makedirs(img_dir, exist_ok=True)

        timing = self.execute_command(cmd, capture=True)

        files = [
            f for f in os.listdir(img_dir) if os.path.isfile(os.path.join(img_dir, f))
        ]
        print(files)
        upload_time = self.datasource.upload(
            bucket_name, "extract-data/step3_out", img_dir
        )

        return {
            "result": image_dir,
            "stats": {
                "execution": timing,
                "download_time": download_time,
                "download_size": download_size,
                "upload_time": upload_time,
                "upload_size": self.get_size(img_dir),
            },
        }
