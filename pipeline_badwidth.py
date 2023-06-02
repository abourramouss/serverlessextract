from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource
import time
import matplotlib.pyplot as plt


if "__main__" == __name__:
    #Pipeline parameters
    
    steps = [RebinningStep('extract-data/parameters/STEP1-flagrebin.parset', 'rebinning.lua'), 
             CalibrationStep('extract-data/parameters/STEP2A-calibration.parset',
                             'extract-data/parameters/STEP2A-apparent.skymodel',
                             'extract-data/parameters/apparent.sourcedb'
                             ),
             SubtractionStep('extract-data/parameters/STEP2B-subtract.parset',
                             'extract-data/parameters/apparent.sourcedb'
                             ),
            ]
    executor = LithopsExecutor()
    measurement_sets_all = ['extract-data/partitions/partition_1.ms',
                            'extract-data/partitions/partition_2.ms', 
                            'extract-data/partitions/partition_3.ms',
                            'extract-data/partitions/partition_4.ms',
                            'extract-data/partitions/partition_5.ms', 
                            'extract-data/partitions/partition_6.ms', 
                            'extract-data/partitions/partition_7.ms', 
                            'extract-data/partitions/partition_8.ms', 
                            'extract-data/partitions/partition_9.ms',
                            'extract-data/partitions/partition_10.ms',
                            'extract-data/partitions/partition_11.ms']
    bucket_name = 'aymanb-serverless-genomics'
    output_dir = '/tmp/'
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    datasource = LithopsDataSource()
    # Configure LithopsExecutor
    executor = LithopsExecutor ()
    total_data_size = 0
    
    
    partition_counts = []
    bandwidths = []

    for i in range(1, len(measurement_sets_all) + 1):
        measurement_sets = measurement_sets_all[:i]
        
      
        total_data_size = 0
        for ms in measurement_sets:
            total_data_size += datasource.get_ms_size(bucket_name, ms)
        # Convert to MB
        total_data_size = total_data_size / (1024 * 1024)
        start_time = time.time()
        #Step 1: Flagging and rebinning the data
        calibration_data = executor.execute(steps[0], measurement_sets, extra_args=extra_args, extra_env=extra_env)
        end_time = time.time()

        # Compute bandwidth
        time_taken = end_time - start_time
        bandwidth = total_data_size / time_taken

        # Store results
        partition_counts.append(i)  # i is also the number of workers
        bandwidths.append(bandwidth)

    # Plot results
    plt.plot(partition_counts, bandwidths)
    plt.xlabel('Number of Partitions/Workers')
    plt.ylabel('Bandwidth MB/s')
    plt.title('Bandwidth vs. Number of Partitions/Workers for Rebinning Step')
    plt.savefig('bandwidth_plot.png')
