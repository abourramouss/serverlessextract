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
    total_data_size = sum([datasource.get_ms_size(bucket_name, ms) for ms in calibration_data]) / (1024 * 1024) # convert to MB
    time_taken = end_time - start_time # in seconds
    bandwidth = total_data_size / time_taken # MB/s
    bandwidths.append(bandwidth)
    print(f"Step 1 Bandwidth: {bandwidth} MB/s")

    # Step 2a: Calibration solutions computation
    start_time = time.time()
    substraction_data = executor.execute(
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
    time_taken = end_time - start_time # in seconds
    bandwidth = total_data_size / time_taken # MB/s
    bandwidths.append(bandwidth)
    print(f"Step 2a Bandwidth: {bandwidth} MB/s")

    # Step 2b: Subtracting strong sources
    start_time = time.time()
    substracted_measurement_sets = executor.execute(
        SubtractionStep(
            'extract-data/parameters/STEP2B-subtract.parset',
            'extract-data/parameters/apparent.sourcedb'
        ), 
        calibration_data, 
        extra_args=extra_args, 
        extra_env=extra_env
    )
    end_time = time.time()
    time_taken = end_time - start_time # in seconds
    bandwidth = total_data_size / time_taken # MB/s
    bandwidths.append(bandwidth)
    print(f"Step 2b Bandwidth: {bandwidth} MB/s")



      # Step 2c: Applying calibration solutions
    start_time = time.time()
    calibrated_mss = executor.execute(
        ApplyCalibrationStep(
            'extract-data/parameters/STEP2C-applycal.parset'
        ), 
        substracted_measurement_sets, 
        extra_args=extra_args, 
        extra_env=extra_env
    )
    end_time = time.time()
    time_taken = end_time - start_time # in seconds
    bandwidth = total_data_size / time_taken # MB/s
    bandwidths.append(bandwidth)
    print(f"Step 2c Bandwidth: {bandwidth} MB/s")

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
    
    time_taken = end_time - start_time # in seconds
    bandwidth = total_data_size / time_taken # MB/s
    bandwidths.append(bandwidth)
    print(f"Step 3 Bandwidth: {bandwidth} MB/s")
    
    # Plotting
    plt.figure(figsize=(10, 6))
    plt.bar(step_names, bandwidths, color='blue')
    plt.xlabel('Steps')
    plt.ylabel('Bandwidth (MB/s)')
    plt.title('Bandwidth for each step')
    
    # Saving plot as image
    plt.savefig('bandwidth_plot.png', dpi=300)
