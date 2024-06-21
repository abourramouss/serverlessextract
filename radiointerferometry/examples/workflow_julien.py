import time
import logging
import lithops
from radiointerferometry.utils import setup_logging, get_executor_id_lithops
from radiointerferometry.steps.imaging import ImagingStep
from radiointerferometry.steps.pipelinestep import DP3Step
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.partitioning import StaticPartitioner


# Logger setup
LOG_LEVEL = logging.INFO
logger = setup_logging(LOG_LEVEL)
partitioner = StaticPartitioner(log_level=LOG_LEVEL)

BUCKET = "os-10gb"
RACK_BUCKET = "os-10gb"


def prepend_hash_to_key(key: str) -> str:
    # print(f"Executor ID: {get_executor_id_lithops()}")
    return f"440531/{key}"


fexec = lithops.FunctionExecutor(
    log_level=LOG_LEVEL, runtime_memory=2048, runtime_cpu=4
)

# Input ms's are stored here
inputs = InputS3(
    bucket=BUCKET, key="CYGLOOP2024/20240312_081800_20240312_084100_CYGLOOP_CYGA/"
)

print(prepend_hash_to_key("dummy_key"))


# Workflow is described like this:
# CALIBRATOR:  [FLAG&REBIN] -> [CALIBRATION] -> (caltables HDF5 files)
# TARGET: [FLAG&REBIN] -> [CALIBRATION (APPLYCAL ONLY)] -> [IMAGING]

# Rebinning parameters with hash included in the key as a root directory, notice how we use the result from the partitioning step

# CALIBRATOR REBINNING PARAMS
CAL_rebinning_params = {
    "msin": inputs,
    "steps": "[aoflag, avg, count]",
    "aoflag.type": "aoflagger",
    "aoflag.strategy": InputS3(
        bucket=BUCKET,
        key="parameters/rebinning/STEP1-NenuFAR64C1S.lua",
    ),
    "avg.type": "averager",
    "avg.freqstep": 5,
    "avg.timestep": 2,
    "msout": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/rebinning_out/ms"),
        file_ext="ms",
    ),
    "numthreads": 4,
    "log_output": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/rebinning_out/logs"),
        file_ext="log",
    ),
}


# CALIBRATOR REBINNING
start_time = time.time()
finished_job = DP3Step(parameters=CAL_rebinning_params, log_level=LOG_LEVEL).run(
    func_limit=1
)

end_time = time.time()
logger.info(f"CAL Rebinning completed in {end_time - start_time} seconds.")
