import time
import logging
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from steps.agg_cal import CalibrationSubstractionApplyCalibrationStep
from steps.pipelinestep import DP3Step
from s3path import S3Path
from profiling import JobCollection
from lithops import Storage
from datasource import InputS3, OutputS3


log_format = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d -- %(message)s"

# Configure logging with the custom format
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


rebinning_params = {
    "msin": InputS3(
        bucket="ayman-extract",
        key="partitions/partitions_7900_20zip_1/",
    ),
    "steps": "[aoflag, avg, count]",
    "aoflag.type": "aoflagger",
    "aoflag.strategy": InputS3(
        bucket="ayman-extract",
        key="parameters/rebinning/STEP1-NenuFAR64C1S.lua",
    ),
    "avg.type": "averager",
    "avg.freqstep": 4,
    "avg.timestep": 8,
    "msout": OutputS3(
        bucket="ayman-extract", key="extract-data/rebinning_out", file_ext="ms"
    ),
}

start_time = time.time()
finished_job = DP3Step(parameters=rebinning_params).run(func_limit=1)
end_time = time.time()

"""


logger.info(f"Rebinning completed in {end_time - start_time} seconds.")


calibration_params = {
    "msin": InputS3(
        bucket="ayman-extract",
        key="extract-data/rebinning_out",
    ),
    "msin.datacolumn": "DATA",
    "msout": ".",
    "steps": "[cal, sub]",
    "cal.type": "ddecal",
    "cal.mode": "diagonal",
    "cal.sourcedb": InputS3(
        bucket="ayman-extract",
        key="parameters/calibration/STEP2A-apparent.sourcedb",
    ),
    "cal.h5parm": OutputS3(
        bucket="ayman-extract",
        key="extract-data/calibration_out",
        naming_pattern="calibration_{id}.h5",
    ),
    "numthreads": 4,
    "cal.solint": 4,
    "cal.nchan": 4,
    "cal.maxiter": 50,
    "cal.uvlambdamin": 5,
    "cal.smoothnessconstraint": 2e6,
    "msout.datacolumn": "SUBTRACTED_DATA",
    "steps": "[sub]",
    "sub.type": "h5parmpredict",
    "sub.sourcedb": InputS3(
        bucket="ayman-extract", key="parameters/calibration/STEP2A-apparent.sourcedb"
    ),
    "sub.directions": "[[CygA],[CasA]]",
    "sub.operation": "subtract",
    "sub.applycal.parmdb": "",
    "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
    "sub.applycal.correction": "fulljones",
    "sub.applycal.sub_apply_amp.correction": "amplitude000",
    "sub.applycal.sub_apply_phase.correction": "phase000",
}


start_time = time.time()
finished_job = DP3Step(parameters=calibration_params).run(func_limit=1)
end_time = time.time()

logger.info(f"Calibration completed in {end_time - start_time} seconds.")
"""
