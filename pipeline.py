from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import imaging, monitor_and_run_imaging
from s3path import S3Path
from profiling import ProfilerCollection
from lithops import Storage
from plot import aggregate_and_plot, plot_gantt, average_and_plot

parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_15zip"),
        "parameters": {
            "flagrebin": {
                "steps": "[aoflag, avg, count]",
                "aoflag.type": "aoflagger",
                "aoflag.memoryperc": 90,
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
        "input_data_path": S3Path("/ayman-extract/extract-data/applycal_out/ms/"),
        "output_path": S3Path("/ayman-extract/extract-data/imaging_out/"),
    },
}
# 61, 30, 15, 9, 7, 3, 2
# 1769, 3538, 5308, 7076, 10240
# Constants
MB = 1024 * 1024


storage = Storage()

file_path = "profilers_data.json"
collection = ProfilerCollection().load_from_file(file_path)


runtime_memory = 3008
chunk_size = storage.head_object(
    parameters["RebinningStep"]["input_data_path"].bucket,
    f"{parameters['RebinningStep']['input_data_path'].key}/partition_1.ms.zip",
)

chunk_size = int(chunk_size["content-length"]) // MB


print("Chunk size:", chunk_size)
# Instantiate the singleton Profilercollection
rebinning_profilers = RebinningStep(
    input_data_path=S3Path(parameters["RebinningStep"]["input_data_path"]),
    parameters=parameters["RebinningStep"]["parameters"],
    output=parameters["RebinningStep"]["output"],
).run(func_limit=1, runtime_memory=runtime_memory)

collection.add_step_profiler(
    RebinningStep.__name__, runtime_memory, chunk_size, rebinning_profilers
)

collection.save_to_file(file_path)

aggregate_and_plot(
    collection, "rebinning", "rebinning_aggregate.png", runtime_memory, chunk_size
)
average_and_plot(
    collection, "rebinning", "rebinning_average.png", runtime_memory, chunk_size
)
plot_gantt(collection, "rebinning", "rebinning_gantt.png", runtime_memory, chunk_size)
