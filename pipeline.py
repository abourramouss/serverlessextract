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
                             'extract-data/parameters/STEP1-apparent.sourced'
                             ),
             
            ]
    executor = LithopsExecutor()
    mesurement_sets = ['extract-data/partition_1.ms','extract-data/partition_2.ms']
    bucket_name = 'aymanb-serverless-genomics'
    output_dir = '/tmp/'
    extra_env = {"HOME": "/tmp"}

    #Step 1: Flagging and rebinning the data
    extra_args = [bucket_name, output_dir]
    calibration_data = executor.execute(steps[0], mesurement_sets, extra_args=extra_args, extra_env=extra_env)
    
    #Step 2a: Calibration solutions computation
    extra_args = [bucket_name, output_dir]
    executor.execute(steps[1], calibration_data, extra_args=extra_args, extra_env=extra_env)
    

