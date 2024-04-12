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


start_time = time.time()

rebinning_params = {
    "flagrebin": {
        "msin": InputS3(
            bucket="ayman-extract",
            key="partitions/partitions_7900_20zip",
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
            bucket="ayman-extract",
            key="extract-data/rebinning_out",
            naming_pattern="partition_{id}.ms.zip",
        ),
    }
}
# finished_job = DP3Step(parameters=rebinning_params["flagrebin"]).run(func_limit=1)


end_time = time.time()


logger.info(f"Rebinning completed in {end_time - start_time} seconds.")


calibration_params = {
    "cal": {
        "msin": InputS3(
            bucket="ayman-extract",
            key="extract-data/rebinning_out",
        ),
        "msin.datacolumn": "DATA",
        "msout": ".",
        "steps": "[cal]",
        "cal.type": "ddecal",
        "cal.mode": "diagonal",
        "cal.sourcedb": InputS3(
            bucket="ayman-extract",
            key="parameters/calibration/STEP2A-apparent.sourcedb",
        ),
        "cal.h5parm": OutputS3(
            bucket="ayman-extract",
            key="extract-data/calibration_out",
            naming_pattern="calibration_{id}.h5.zip",
        ),
        "numthreads": 4,
        "cal.solint": 4,
        "cal.nchan": 4,
        "cal.maxiter": 50,
        "cal.uvlambdamin": 5,
        "cal.smoothnessconstraint": 2e6,
    }
}

start_time = time.time()
# Assuming DP3Step is properly defined with a method `run` that can accept `func_limit`
finished_job = DP3Step(parameters=calibration_params["cal"]).run(func_limit=1)
end_time = time.time()

logger.info(f"Calibration completed in {end_time - start_time} seconds.")
