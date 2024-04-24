import time
import logging
from steps.imaging import ImagingStep
from steps.pipelinestep import DP3Step

from datasource import InputS3, OutputS3


log_format = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d -- %(message)s"

# Configure logging with the custom format
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


"""
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
    "numthreads": 4,
}

# TIME TAKEN WITHOUT SPECIFYING THREADS:50, 4 threads: 36
start_time = time.time()
finished_job = DP3Step(parameters=rebinning_params).run(func_limit=1)
end_time = time.time()


logger.info(f"Rebinning completed in {end_time - start_time} seconds.")

calibration_params = {
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
        key="extract-data/applycal_out/h5",
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
        key="extract-data/applycal_out/ms",
        file_ext="ms",
    ),
}


substraction = {
    "msin": InputS3(bucket="ayman-extract", key="extract-data/applycal_out/ms"),
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
        key="extract-data/applycal_out/h5",
        dynamic=True,
        file_ext="h5",
    ),
    "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
    "sub.applycal.correction": "fulljones",
    "sub.applycal.sub_apply_amp.correction": "amplitude000",
    "sub.applycal.sub_apply_phase.correction": "phase000",
    "msout": OutputS3(
        bucket="ayman-extract",
        key="extract-data/applycal_out/ms",
        file_ext="ms",
    ),
    "stdout": OutputS3(
        bucket="ayman-extract",
        key="extract-data/applycal_out/stdout",
        file_ext="txt",
    ),
}





agg_cal = [calibration_params, substraction, apply_calibration]

start_time = time.time()
finished_job = DP3Step(parameters=agg_cal).run(func_limit=1)
end_time = time.time()

logger.info(f"Calibration completed in {end_time - start_time} seconds.")






"""

# Imaging

apply_calibration = {
    "msin": InputS3(bucket="ayman-extract", key="extract-data/applycal_out/ms"),
    "msin.datacolumn": "SUBTRACTED_DATA",
    "msout": OutputS3(
        bucket="ayman-extract",
        key="extract-data/applycal_out/ms",
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
        key="extract-data/applycal_out/h5",
        dynamic=True,
        file_ext="h5",
    ),
}
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
    "/tmp/Cygloop-205-210-b0-1024",
]

start_time = time.time()
finished_job = ImagingStep(
    input_data_path=InputS3(bucket="ayman-extract", key="extract-data/applycal_out/ms"),
    parameters=imaging_params,
    output=OutputS3(
        bucket="ayman-extract", key="extract-data/imaging_out/", file_ext="fits"
    ),
).run()


