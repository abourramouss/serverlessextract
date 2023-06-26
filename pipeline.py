from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource
import time


if "__main__" == __name__:
    # Pipeline parameters
    executor = LithopsExecutor()
    bucket_name = 'aymanb-serverless-genomics'
    prefix = 'extract-data/partitions/'
    output_dir = '/tmp/'
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    datasource = LithopsDataSource()
    all_keys = datasource.storage.list_keys(bucket_name, prefix)

    # Filter keys that include '.ms' in the directory name
    measurement_sets = [key for key in all_keys if '.ms' in key]
    measurement_sets = list(
        set('/'.join(key.split('/')[:3]) for key in measurement_sets))
    print(measurement_sets)
    steps = [
        RebinningStep(
            'extract-data/parameters/STEP1-flagrebin.parset',
            'rebinning.lua'
        ),
        CalibrationStep(
            'extract-data/parameters/STEP2A-calibration.parset',
            'extract-data/parameters/STEP2A-apparent.skymodel',
            'extract-data/parameters/apparent.sourcedb'
        ),
        SubtractionStep(
            'extract-data/parameters/STEP2B-subtract.parset',
            'extract-data/parameters/apparent.sourcedb'
        ),

    ]

    # Step 1: Flagging and rebinning the data
    calibration_data = executor.execute_steps(
        steps, measurement_sets, extra_args=extra_args, extra_env=extra_env)

    # Step 2a: Calibration solutions computation
    # substraction_data = executor.execute(steps[1], calibration_data, extra_args=extra_args, extra_env=extra_env)

    # Step 2b: Subtracting strong sources
    # substracted_measurement_sets = executor.execute(steps[2], calibration_data, extra_args=extra_args, extra_env=extra_env)

    # Step 2c: Applying calibration solutions
    # calibrated_mss = executor.execute(steps[3], substracted_measurement_sets, extra_args=extra_args, extra_env=extra_env)

    # Step 3: Imaging
    # executor.execute_call_async(steps[4], calibrated_mss, extra_args=extra_args, extra_env=extra_env)
