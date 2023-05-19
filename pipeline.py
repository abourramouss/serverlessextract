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
    steps = [RebinningStep('s3://aymanb-serverless-genomics/extract-data/parameters/STEP1-flagrebin.parset', 'rebinning.lua')]
    executor = LithopsExecutor()
    mesurement_sets = ['extract-data/mesurement_set.ms']
    bucket_name = 'aymanb-serverless-genomics'
    output_dir = '/tmp/'
    
    
    executor.execute(steps[0], mesurement_sets, bucket_name, output_dir)