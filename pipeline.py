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
    #Pipeline parameters
    executor = LithopsExecutor()
    mesurement_sets = ['extract-data/partitions/partition_1.ms',
                        'extract-data/partitions/partition_2.ms', 
                        'extract-data/partitions/partition_3.ms',
                        'extract-data/partitions/partition_4.ms',
                        'extract-data/partitions/partition_5.ms', 
                        'extract-data/partitions/partition_6.ms', 
                        'extract-data/partitions/partition_7.ms', 
                        'extract-data/partitions/partition_8.ms', 
                        'extract-data/partitions/partition_9.ms',
                        'extract-data/partitions/partition_10.ms',
                        'extract-data/partitions/partition_11.ms'
                        ]
    bucket_name = 'aymanb-serverless-genomics'
    output_dir = '/tmp/'
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    datasource = LithopsDataSource()
    
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
            ApplyCalibrationStep(
                        'extract-data/parameters/STEP2C-applycal.parset'
                ),
            ImagingStep(
                        'extract-data/output/image',
            )
            ]
    
  
    #Step 1: Flagging and rebinning the data
    calibration_data = executor.execute(steps[0], mesurement_sets, extra_args=extra_args, extra_env=extra_env)

    #Step 2a: Calibration solutions computation
    substraction_data = executor.execute(steps[1], calibration_data, extra_args=extra_args, extra_env=extra_env)
    
    #Step 2b: Subtracting strong sources
    substracted_mesurement_sets = executor.execute(steps[2], calibration_data, extra_args=extra_args, extra_env=extra_env)

    print(substracted_mesurement_sets)
    #Step 2c: Applying calibration solutions
    calibrated_mss = executor.execute(steps[3], substracted_mesurement_sets, extra_args=extra_args, extra_env=extra_env)
    
    print(calibrated_mss)
    
    #Step 3: Imaging
    executor.execute_call_async(steps[4], calibrated_mss, extra_args=extra_args, extra_env=extra_env)