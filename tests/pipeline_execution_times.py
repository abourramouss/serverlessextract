import matplotlib.pyplot as plt
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
    measurement_sets = list(set('/'.join(key.split('/')[:3]) for key in measurement_sets))

    step_names = ['Rebinning', 'Calibration', 'Subtraction', 'Apply Calibration', 'Imaging']
    bandwidths = []
    execution_times = []

    # Step 1: Flagging and rebinning the data
    start_time = time.time()
    calibration_data = executor.execute(
        RebinningStep(
            'extract-data/parameters/STEP1-flagrebin.parset',
            'rebinning.lua'
        ),
        measurement_sets,
        extra_args=extra_args,
        extra_env=extra_env
    )
    end_time = time.time()
    total_data_size = sum([datasource.get_ms_size(bucket_name, ms) for ms in calibration_data]) / (1024 * 1024)  # convert to MB
    time_taken = end_time - start_time  # in seconds
    bandwidth = total_data_size / time_taken  # MB/s
    bandwidths.append(bandwidth)
    execution_times.append(time_taken)
    print(f"Step 1 Bandwidth: {bandwidth} MB/s")
    print(f"Step 1 Execution Time: {time_taken} seconds")

    # Step 2a: Calibration solutions computation
    start_time = time.time()
    subtraction_data = executor.execute(
        CalibrationStep(
            'extract-data/parameters/STEP2A-calibration.parset',
            'extract-data/parameters/STEP2A-apparent.skymodel',
            'extract-data/parameters/apparent.sourcedb'
        ),
        calibration_data,
        extra_args=extra_args,
        extra_env=extra_env
    )
    end_time = time.time()
    time_taken = end_time - start_time  # in seconds
    bandwidth = total_data_size / time_taken  # MB/s
    bandwidths.append(bandwidth)
    execution_times.append(time_taken)
    print(f"Step 2a Bandwidth: {bandwidth} MB/s")
    print(f"Step 2a Execution Time: {time_taken} seconds")

    # Step 2b: Subtracting strong sources
    start_time = time.time()
    subtracted_measurement_sets = executor.execute(
        SubtractionStep(
            'extract-data/parameters/STEP2B-subtract.parset',
            'extract-data/parameters/apparent.sourcedb'
        ),
        calibration_data,
        extra_args=extra_args,
        extra_env=extra_env
    )
    end_time = time.time()
    time_taken = end_time - start_time  # in seconds
    bandwidth = total_data_size / time_taken  # MB/s
    bandwidths.append(bandwidth)
    execution_times.append(time_taken)
    print(f"Step 2b Bandwidth: {bandwidth} MB/s")
    print(f"Step 2b Execution Time: {time_taken} seconds")

    # Step 2c: Applying calibration solutions
    start_time = time.time()
    calibrated_mss = executor.execute(
        ApplyCalibrationStep(
            'extract-data/parameters/STEP2C-applycal.parset'
        ),
        subtracted_measurement_sets,
        extra_args=extra_args,
        extra_env=extra_env
    )
    end_time = time.time()
    time_taken = end_time - start_time  # in seconds
    bandwidth = total_data_size / time_taken  # MB/s
    bandwidths.append(bandwidth)
    execution_times.append(time_taken)
    print(f"Step 2c Bandwidth: {bandwidth} MB/s")
    print(f"Step 2c Execution Time: {time_taken} seconds")

    # Step 3: Imaging
    start_time = time.time()
    imaging_result = executor.execute_call_async(
        ImagingStep(
            'extract-data/output/image',
        ),
        calibrated_mss,
        extra_args=extra_args,
        extra_env=extra_env
    )
    end_time = time.time()

    time_taken = end_time - start_time  # in seconds

    bandwidth = total_data_size / time_taken  # MB/s
    bandwidths.append(bandwidth)
    execution_times.append(time_taken)
    print(f"Step 3 Bandwidth: {bandwidth} MB/s")
    print(f"Step 3 Execution Time: {time_taken} seconds")

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.bar(step_names, execution_times, color='blue')
    plt.xlabel('Steps')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time for each step')

    # Saving plot as image
    plt.savefig('execution_time_plot.png', dpi=300)
    
    # Display execution times
    print(f"\nExecution Times for each step:")
    for step, time_taken in zip(step_names, execution_times):
        print(f"{step}: {time_taken} seconds")