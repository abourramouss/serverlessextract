import time
import logging
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from steps.agg_cal import CalibrationSubstractionApplyCalibrationStep
from s3path import S3Path
from profiling import JobCollection
from lithops import Storage

"""
from plot import (
    aggregate_and_plot,
    plot_gantt,
    average_and_plot,
    plot_cost_vs_time_from_collection,
    plot_cost_vs_time_pareto_simulated,
    plot_cost_vs_time_pareto_real,
    plot_speedup_vs_cost_from_collection,
    plot_memory_speedup_from_collection,
    plot_cost_vs_time_pareto_real_partition,
    plot_cost_vs_time_pareto_real_ec2,
)
"""
log_format = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d -- %(message)s"

# Configure logging with the custom format
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger(__name__)

MB = 1024 * 1024

parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_7900_14zip"),
        "parameters": {
            "flagrebin": {
                "steps": "[aoflag, avg, count]",
                "aoflag.type": "aoflagger",
                "aoflag.strategy": S3Path(
                    "/ayman-extract/parameters/rebinning/STEP1-NenuFAR64C1S.lua"
                ),
                "avg.type": "averager",
                "avg.freqstep": 4,
                "avg.timestep": 8,
            }
        },
        "output": S3Path("/ayman-extract/extract-data/rebinning_out/"),
    },
    "CalibrationStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/rebinning_out/"),
        "parameters": {
            "cal": {
                "msin": "",
                "msin.datacolumn": "DATA",
                "msout": ".",
                "steps": "[cal]",
                "cal.type": "ddecal",
                "cal.mode": "diagonal",
                "cal.sourcedb": S3Path(
                    "/ayman-extract/parameters/calibration/STEP2A-apparent.sourcedb"
                ),
                "cal.h5parm": "",
                "cal.solint": 4,
                "cal.nchan": 4,
                "cal.maxiter": 50,
                "cal.uvlambdamin": 5,
                "cal.smoothnessconstraint": 2e6,
            }
        },
        "output": S3Path("/ayman-extract/extract-data/calibration_out/"),
    },
    "SubstractionStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/calibration_out/"),
        "parameters": {
            "sub": {
                "msin": "",
                "msin.datacolumn": "DATA",
                "msout": ".",
                "msout.datacolumn": "SUBTRACTED_DATA",
                "steps": "[sub]",
                "sub.type": "h5parmpredict",
                "sub.sourcedb": S3Path(
                    "/ayman-extract/parameters/calibration/STEP2A-apparent.sourcedb"
                ),
                "sub.directions": "[[CygA],[CasA]]",
                "sub.operation": "subtract",
                "sub.applycal.parmdb": "",
                "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
                "sub.applycal.correction": "fulljones",
                "sub.applycal.sub_apply_amp.correction": "amplitude000",
                "sub.applycal.sub_apply_phase.correction": "phase000",
            }
        },
        "output": S3Path("/ayman-extract/extract-data/substraction_out/"),
    },
    "ApplyCalibrationStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/substraction_out/"),
        "parameters": {
            "apply": {
                "msin": "",
                "msin.datacolumn": "SUBTRACTED_DATA",
                "msout": ".",
                "msout.datacolumn": "CORRECTED_DATA",
                "steps": "[apply]",
                "apply.type": "applycal",
                "apply.steps": "[apply_amp,apply_phase]",
                "apply.apply_amp.correction": "amplitude000",
                "apply.apply_phase.correction": "phase000",
                "apply.direction": "[Main]",
                "apply.parmdb": "",
            }
        },
        "output": S3Path("/ayman-extract/extract-data/applycal_out/"),
    },
    "ImagingStep": {
        "input_data_path": S3Path("/ayman-extract/extract-data/applycal_out/"),
        "output_path": S3Path("/ayman-extract/extract-data/imaging_out/"),
    },
}


file_path = "profilers.json"

cpus_per_worker = 2
mem = 4000
storage = Storage()


print(parameters["RebinningStep"]["input_data_path"])
chunk_size = storage.head_object(
    parameters["RebinningStep"]["input_data_path"].bucket,
    f"{parameters['RebinningStep']['input_data_path'].key}/partition_1.ms.zip",
)
chunk_size = int(chunk_size["content-length"]) // MB

logger.info(f"Chunk size: {chunk_size}")
logger.info(f"Runtime memory: {mem}")
logger.info(f"CPUs per worker: {cpus_per_worker}")

collection = JobCollection().load_from_file(file_path)

start_time = time.time()
finished_job = RebinningStep(
    input_data_path=parameters["RebinningStep"]["input_data_path"],
    parameters=parameters["RebinningStep"]["parameters"],
    output=parameters["RebinningStep"]["output"],
).run(
    chunk_size=chunk_size,
    runtime_memory=mem,
    cpus_per_worker=cpus_per_worker,
    func_limit=8,
)

end_time = time.time()
rebinning_time = end_time - start_time


start_time = time.time()
finished_job = CalibrationSubstractionApplyCalibrationStep(
    input_data_path=parameters["RebinningStep"]["output"],
    parameters=[
        parameters["CalibrationStep"]["parameters"],
        parameters["SubstractionStep"]["parameters"],
        parameters["ApplyCalibrationStep"]["parameters"],
    ],
    output=parameters["ApplyCalibrationStep"]["output"],
).run(
    chunk_size=chunk_size,
    runtime_memory=mem,
    cpus_per_worker=cpus_per_worker,
)
end_time = time.time()

calibration_time = end_time - start_time

start_time = time.time()

finished_job = ImagingStep(
    input_data_path=parameters["ImagingStep"]["input_data_path"],
    parameters="",
    output=parameters["ImagingStep"]["output_path"],
).run(
    chunk_size=chunk_size,
    runtime_memory=10000,
    cpus_per_worker=10,
)

end_time = time.time()

imaging_time = end_time - start_time

logger.info(f"Chunk size: {chunk_size}")
logger.info(f"Runtime memory: {mem}")
logger.info(f"CPUs per worker: {cpus_per_worker}")

logger.info(f"Rebinning time: {rebinning_time}")
logger.info(f"Calibration time: {calibration_time}")
logger.info(f"Imaging time: {end_time-start_time}")

"""

collection.add_job(RebinningStep.__name__, finished_job)
collection.save_to_file(file_path)
plot_cost_vs_time_from_collection(collection, "rebinning/cost_vs_time")
average_and_plot("RebinningStep", collection, finished_job)
plot_gantt(
    collection,
    f"rebinning_m7i/gantt/chunk_size{chunk_size}",
    f"rebinning_gantt_runtime_{mem}.png",
    mem,
    chunk_size,
)


plot_cost_vs_time_pareto_real_ec2(
    collection,
    "rebinning_m7i/cost_vs_time_pareto_real_partition",
    "RebinningStep",
    7603,
)
"""
