from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource


if "__main__" == __name__:
    #Pipeline parameters
    
    steps = [RebinningStep('extract-data/parameters/STEP1-flagrebin.parset', 'rebinning.lua'), 
             CalibrationStep('extract-data/parameters/STEP2A-calibration.parset',
                             'extract-data/parameters/STEP2A-apparent.skymodel',
                             'extract-data/parameters/STEP1-apparent.sourcedb'
                             ),
             SubtractionStep('extract-data/parameters/STEP2B-subtract.parset',
                             'extract-data/parameters/STEP1-apparent.sourcedb'
                             ),
            ]
    executor = LithopsExecutor()
    mesurement_sets = ['extract-data/partitions/partition_1.ms','extract-data/partitions/partition_2.ms', 'extract-data/partitions/partition_3.ms', 'extract-data/partitions/partition_4.ms']
    bucket_name = 'aymanb-serverless-genomics'
    output_dir = '/tmp/'
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    
    #Step 1: Flagging and rebinning the data
    calibration_data = executor.execute(steps[0], mesurement_sets, extra_args=extra_args, extra_env=extra_env)
    
    #Step 2a: Calibration solutions computation
    substraction_data = executor.execute(steps[1], calibration_data, extra_args=extra_args, extra_env=extra_env)
    
    #Step 2b: Subtracting strong sources
    executor.execute(steps[2], calibration_data, extra_args=extra_args, extra_env=extra_env)

