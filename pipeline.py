from executors import LithopsExecutor
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import numpy as np




if "__main__" == __name__:
    # Pipeline parameters
    executor = LithopsExecutor()
    bucket_name = "extract"
    prefix = "partitions/partition"
    output_dir = "/tmp/"
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    datasource = LithopsDataSource()
    all_keys = datasource.storage.list_keys(bucket_name, prefix)
    print(all_keys)
    # Filter keys that include '.ms' in the directory name
    measurement_sets = [key for key in all_keys if ".ms" in key]
    measurement_sets = list(
        set("/".join(key.split("/")[:3]) for key in measurement_sets)
    )
    map = [
        RebinningStep(
            "/parameters/STEP1-flagrebin.parset", "/parameters/STEP1-NenuFAR64C1S.lua"
        ),
    ]

    print("Executing pipeline")
    # Execute all the steps that can be executed in parallel in a single worker.
    results_and_timings = executor.execute_steps(
        map, measurement_sets, extra_args=extra_args, extra_env=extra_env
    )

