from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource
import time
import pandas as pd
import matplotlib.pyplot as plt


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
    stats_list = []
    for result_and_timing in results_and_timings:
        print(f"Result: {result_and_timing['result']}")
        print(f"Stats: {result_and_timing['stats']}")
        calibrated_ms.append(result_and_timing['result'])
        stats_list.append(result_and_timing['stats'])

    print(f"Calibrated MS: {calibrated_ms}")
    # Imaging step: Reduce Phase
    imaging_stats = executor.execute_call_async(
        reduce, calibrated_ms, extra_args=extra_args, extra_env=extra_env)

    print(f"Imaging Stats: {imaging_stats}")

    execution_times = []
    io_times = []
    io_sizes = []

    for i, stats in enumerate(stats_list):
        for step_name, step_data in stats.items():
            execution_times.append(
                {"worker": i, "step": step_name, "time": step_data.get('execution', 0)})

            if 'download_time' in step_data:
                io_times.append({"worker": i, "step": f"{step_name}_download",
                                "time": step_data['download_time']})
                io_sizes.append({"worker": i, "step": f"{step_name}_download",
                                "size": step_data['download_size']})

            if 'upload_time' in step_data:
                io_times.append({"worker": i, "step": f"{step_name}_upload",
                                "time": step_data['upload_time']})
                io_sizes.append({"worker": i, "step": f"{step_name}_upload",
                                "size": step_data['upload_size']})
    execution_times_df = pd.DataFrame(execution_times)
    io_times_df = pd.DataFrame(io_times)
    io_sizes_df = pd.DataFrame(io_sizes)

    fig, axes = plt.subplots(3, 1, figsize=(10, 15))

    execution_times_df.pivot(index='worker', columns='step',
                             values='time').plot(kind='bar', ax=axes[0])
    axes[0].set_title('Execution Times')
    axes[0].set_ylabel('Time (s)')

    io_times_df.pivot(index='worker', columns='step',
                      values='time').plot(kind='bar', ax=axes[1])
    axes[1].set_title('I/O Times')
    axes[1].set_ylabel('Time (s)')

    io_sizes_df.pivot(index='worker', columns='step',
                      values='size').plot(kind='bar', ax=axes[2])
    axes[2].set_title('I/O Sizes')
    axes[2].set_ylabel('Size (MB)')

    plt.tight_layout()
    plt.savefig('worker_stats.png')
