import time
import logging
import lithops
from radiointerferometry.utils import setup_logging, get_executor_id_lithops
from radiointerferometry.steps.imaging import ImagingStep
from radiointerferometry.steps.pipelinestep import DP3Step
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.partitioning import StaticPartitioner
from radiointerferometry.profiling import (
    CompletedWorkflow,
    CompletedWorkflowsCollection,
)


# Logger setup
LOG_LEVEL = logging.INFO
logger = setup_logging(LOG_LEVEL)
partitioner = StaticPartitioner(log_level=LOG_LEVEL)


def prepend_hash_to_key(key: str) -> str:
    return f"{get_executor_id_lithops()}/{key}"


fexec = lithops.FunctionExecutor( log_level=LOG_LEVEL)

# Input ms's are stored here
inputs = InputS3(bucket="ayman-extract", key="partitions/partition_1/")

# Where to store the output ms's after partitioning
msout = OutputS3(bucket="ayman-extract", key=f"partitions/partitions_total_20zip/")




# The partitioning params are the input ms, the number of partitions, and the output ms.
partitioning_params = {
    "msin": inputs,
    "num_partitions": 20,
    "msout": msout,
}

future = fexec.call_async(partitioner.partition_ms, partitioning_params)

result = fexec.get_result()

logger.info(f"Partitioning result: {result}")


# Rebinning parameters, partitioning results are sent to msin

rebinning_params = {
    "msin": result,
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
        key=prepend_hash_to_key("applycal_out/ms"),
        file_ext="ms",
    ),
    "numthreads": 4,
    "log_output": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("rebinning_out/logs"),
        file_ext="log",
    ),
}


# Calibration parameters with hash included in the key as a root directory
calibration_params = {
    "msin": InputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/ms"),
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
        key=prepend_hash_to_key("applycal_out/cal/h5"),
        file_ext="h5",
    ),
    "cal.solint": 4,
    "cal.nchan": 4,
    "cal.maxiter": 50,
    "cal.uvlambdamin": 5,
    "cal.smoothnessconstraint": 2e6,
    "numthreads": 4,
    "msout": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/ms"),
        file_ext="ms",
    ),
    "log_output": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/cal/logs"),
        file_ext="log",
    ),
}

# Subtraction parameters with hash included in the key as a root directory
substraction = {
    "msin": InputS3(bucket="ayman-extract", key=prepend_hash_to_key("applycal_out/ms")),
    "msin.datacolumn": "DATA",
    "msout.datacolumn": "SUBTRACTED_DATA",
    "steps": "[sub]",
    "sub.type": "h5parmpredict",
    "sub.sourcedb": InputS3(
        bucket="ayman-extract",
        key="parameters/calibration/STEP2A-apparent.sourcedb",
    ),
    "sub.directions": "[[CygA],[CasA]]",
    "sub.operation": "subtract",
    "sub.applycal.parmdb": InputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/cal/h5"),
        dynamic=True,
        file_ext="h5",
    ),
    "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
    "sub.applycal.correction": "fulljones",
    "sub.applycal.sub_apply_amp.correction": "amplitude000",
    "sub.applycal.sub_apply_phase.correction": "phase000",
    "msout": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/ms"),
        file_ext="ms",
    ),
    "log_output": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/substract/logs"),
        file_ext="log",
    ),
}

# Apply calibration parameters with hash included in the key as a root directory
apply_calibration = {
    "msin": InputS3(bucket="ayman-extract", key=prepend_hash_to_key("applycal_out/ms")),
    "msin.datacolumn": "SUBTRACTED_DATA",
    "msout": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/ms"),
        file_ext="ms",
    ),
    "msout.datacolumn": "CORRECTED_DATA",
    "steps": "[apply]",
    "apply.type": "applycal",
    "apply.steps": "[apply_amp,apply_phase]",
    "apply.apply_amp.correction": "amplitude000",
    "apply.apply_phase.correction": "phase000",
    "apply.direction": "[Main]",
    "apply.parmdb": InputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/cal/h5"),
        dynamic=True,
        file_ext="h5",
    ),
    "log_output": OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("applycal_out/apply/logs"),
        file_ext="log",
    ),
}

# Imaging parameters with hash included in the key as a root directory
imaging_params = [
    "-size",
    "1024",
    "1024",
    "-pol",
    "I",
    "-scale",
    "5arcmin",
    "-niter",
    "100000",
    "-gain",
    "0.1",
    "-mgain",
    "0.6",
    "-auto-mask",
    "5",
    "-local-rms",
    "-multiscale",
    "-no-update-model-required",
    "-make-psf",
    "-auto-threshold",
    "3",
    "-weight",
    "briggs",
    "0",
    "-data-column",
    "CORRECTED_DATA",
    "-nmiter",
    "0",
    "-name",
    OutputS3(
        bucket="ayman-extract",
        key=prepend_hash_to_key("imag_out"),
    ),
]



# Execute Rebinning
start_time = time.time()
rebinning_runner = DP3Step(parameters=rebinning_params, log_level=LOG_LEVEL)

completed_step = rebinning_runner(step_name="rebinning", func_limit=1)

end_time = time.time()

logger.info(f"Rebinning completed in {end_time - start_time} seconds.")



# Execute Calibration
start_time = time.time()
finished_job = DP3Step(
    parameters=[calibration_params, substraction, apply_calibration],
    log_level=LOG_LEVEL,
).run()
end_time = time.time()
logger.info(f"Calibration completed in {end_time - start_time} seconds.")


# Execute Imaging
start_time = time.time()
finished_job = ImagingStep(
    input_data_path=InputS3(
        bucket="ayman-extract", key=prepend_hash_to_key("applycal_out/ms")
    ),
    parameters=imaging_params,
    log_level=LOG_LEVEL,
).run()
end_time = time.time()
logger.info(f"Imaging completed in {end_time - start_time} seconds.")

