import time
import logging
import lithops
from radiointerferometry.utils import setup_logging, get_executor_id_lithops
from radiointerferometry.steps.imaging import ImagingStep
from radiointerferometry.steps.pipelinestep import DP3Step
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.partitioning import StaticPartitioner
from pycompss.api.task import task
from pycompss.api.api import compss_barrier

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


CAL_calibration_params = {
    "msin": InputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/rebinning_out/ms"),
    ),
    "msin.datacolumn": "DATA",
    "msout": ".",
    "steps": "[cal]",
    "cal.type": "gaincal",
    "cal.caltype": "diagonal",
    "cal.sourcedb": InputS3(
        bucket=BUCKET,
        key="parameters/calibration/CAL.sourcedb",
    ),
    "cal.parmdb": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/calibration_out/h5"),
        file_ext="h5",
    ),
    "cal.solint": 0,  # means 1 solution for all time steps
    "cal.nchan": 1,  # means 1 solution per channel
    "cal.maxiter": 50,
    "cal.uvlambdamin": 5,
    "cal.smoothnessconstraint": 2e6,
    "numthreads": 4,
    "log_output": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/calibration_out/logs"),
        file_ext="log",
    ),
}

# CALIBRATOR CALIBRATION
start_time = time.time()

finished_job = DP3Step(parameters=CAL_calibration_params, log_level=LOG_LEVEL).run(
    func_limit=1
)

end_time = time.time()
logger.info(f"CAL Calibration completed in {end_time - start_time} seconds.")


inputs_tar = InputS3(
    bucket=BUCKET, key="CYGLOOP2024/20240312_084100_20240312_100000_CYGLOOP_TARGET/"
)


# TARGET REBINNING PARAMS
TARGET_rebinning_params = {
    "msin": inputs_tar,
    "steps": "[aoflag, avg, count]",
    "aoflag.type": "aoflagger",
    "aoflag.strategy": InputS3(
        bucket=BUCKET,
        key="parameters/rebinning/STEP1-NenuFAR64C1S.lua",
    ),
    "avg.type": "averager",
    "avg.freqstep": 5,  # averaging 5 channels
    "avg.timestep": 2,  # averaging 2 times samples
    "msout": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("TAR/rebinning_out/ms"),
        file_ext="ms",
    ),
    "numthreads": 4,
    "log_output": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("TAR/rebinning_out/logs"),
        file_ext="log",
    ),
}

# TARGET REBINNING
start_time = time.time()
finished_job = DP3Step(parameters=TARGET_rebinning_params, log_level=LOG_LEVEL).run(
    func_limit=1
)

end_time = time.time()
logger.info(f"TARGET Rebinning completed in {end_time - start_time} seconds.")


TARGET_apply_calibration = {
    "msin": InputS3(bucket=BUCKET, key=prepend_hash_to_key("TAR/rebinning_out/ms")),
    "msin.datacolumn": "DATA",
    "msout": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("TAR/rebinning_out/ms"),
        file_ext="ms",
        remote_key_ow=prepend_hash_to_key("TAR/applycal_out/ms"),
    ),
    "msout.datacolumn": "CORRECTED_DATA",
    "steps": "[apply]",
    "apply.type": "applycal",
    "apply.steps": "[apply_amp,apply_phase]",
    "apply.apply_amp.correction": "amplitude000",
    "apply.apply_phase.correction": "phase000",
    "apply.direction": "[Main]",
    "apply.parmdb": InputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("CAL/calibration_out/h5"),
        dynamic=True,
        file_ext="h5",
    ),
    "log_output": OutputS3(
        bucket=BUCKET,
        key=prepend_hash_to_key("TAR/applycal_out/logs"),
        file_ext="log",
    ),
}


# TARGET CALIBRATION (APPLY)
start_time = time.time()
finished_job = DP3Step(parameters=TARGET_apply_calibration, log_level=LOG_LEVEL).run(
    func_limit=1
)

end_time = time.time()
logger.info(f"target Calibration completed in {end_time - start_time} seconds.")


TARGET_imaging_params = [
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
        bucket=BUCKET, key=prepend_hash_to_key("TAR/imag_out/"), file_name="image"
    ),
]
# TARGET IMAGING
start_time = time.time()
finished_job = ImagingStep(
    input_data_path=InputS3(
        bucket=BUCKET, key=prepend_hash_to_key("TAR/applycal_out/ms")
    ),
    parameters=TARGET_imaging_params,
    log_level=LOG_LEVEL,
).run()
end_time = time.time()
logger.info(f"TARGET Imaging completed in {end_time - start_time} seconds.")


@task(returns=int)
def CAL_rebinning():
    # CALIBRATOR REBINNING
    start_time = time.time()
    finished_job = DP3Step(parameters=CAL_rebinning_params, log_level=LOG_LEVEL).run(
        func_limit=1
    )

    end_time = time.time()
    logger.info(f"CAL Rebinning completed in {end_time - start_time} seconds.")
    return 1


@task(returns=int)
def CAL_calibration(cal_rebinning_output):
    # CALIBRATOR CALIBRATION
    start_time = time.time()

    finished_job = DP3Step(parameters=CAL_calibration_params, log_level=LOG_LEVEL).run(
        func_limit=1
    )

    end_time = time.time()
    logger.info(f"CAL Calibration completed in {end_time - start_time} seconds.")
    return 1


@task(returns=int)
def TARGET_rebinning():
    # TARGET REBINNING
    start_time = time.time()
    finished_job = DP3Step(parameters=TARGET_rebinning_params, log_level=LOG_LEVEL).run(
        func_limit=1
    )

    end_time = time.time()
    logger.info(f"TARGET Rebinning completed in {end_time - start_time} seconds.")
    return 1


@task(returns=int)
def TARGET_calibration(target_rebinning_output):
    # TARGET CALIBRATION (APPLY)
    start_time = time.time()
    finished_job = DP3Step(
        parameters=TARGET_apply_calibration, log_level=LOG_LEVEL
    ).run(func_limit=1)

    end_time = time.time()
    logger.info(f"target Calibration completed in {end_time - start_time} seconds.")
    return 1


@task(returns=int)
def TARGET_imaging(target_calibration_output):
    # TARGET IMAGING
    start_time = time.time()
    finished_job = ImagingStep(
        input_data_path=InputS3(
            bucket=BUCKET, key=prepend_hash_to_key("TAR/applycal_out/ms")
        ),
        parameters=TARGET_imaging_params,
        log_level=LOG_LEVEL,
    ).run()
    end_time = time.time()
    logger.info(f"TARGET Imaging completed in {end_time - start_time} seconds.")
    return 1


if __name__ == "__main__":

    # Orchestrate step execution
    rebinning_output = CAL_rebinning()
    calibration_output = CAL_calibration(rebinning_output)
    rebinning_output = TARGET_rebinning()
    calibration_output = TARGET_calibration(rebinning_output)
    imaging_output = TARGET_imaging(calibration_output)
