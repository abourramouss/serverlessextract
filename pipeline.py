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
    map = [
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
        ApplyCalibrationStep(
            'extract-data/parameters/STEP2C-applycal.parset'
        )

    ]

    reduce = ImagingStep(
        'extract-data/output/image',
    )
    # Execute all the steps that can be executed in parallel in a single worker. Reduce Phase.
    results_and_timings = executor.execute_steps(
        map, measurement_sets, extra_args=extra_args, extra_env=extra_env)

    #
    calibrated_ms = []
    for result_and_timing in results_and_timings:
        print(f"Result: {result_and_timing['result']}")
        print(f"Stats: {result_and_timing['stats']}")
        calibrated_ms.append(result_and_timing['result'])

    # print(f"Calibrated MS: {calibrated_ms}")
    # Imaging step: Reduce Phase
    # imaging_stats = executor.execute_call_async(
        # reduce, calibrated_ms, extra_args=extra_args, extra_env=extra_env)

    # print(f"Imaging Stats: {imaging_stats}")
