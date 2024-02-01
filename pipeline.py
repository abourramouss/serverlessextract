import time
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import imaging, monitor_and_run_imaging
from s3path import S3Path
from profiling import JobCollection
from lithops import Storage
from plot import (
    aggregate_and_plot,
    plot_gantt,
    average_and_plot,
    plot_cost_vs_time_from_collection,
    plot_cost_vs_time_pareto_simulated,
    plot_cost_vs_time_pareto_real,
    plot_speedup_vs_cost_from_collection,
    plot_memory_speedup_from_collection,
)


MB = 1024 * 1024

parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_61zip"),
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
        "input_data_path": S3Path("/ayman-extract/extract-data/applycal_out/ms/"),
        "output_path": S3Path("/ayman-extract/extract-data/imaging_out/"),
    },
}


input_data_paths = [
    # "/ayman-extract/partitions/partitions_1100MB_9zip/",
    # "/ayman-extract/partitions/partitions_1100MB_4zip/",
    # "/ayman-extract/partitions/partitions_1100MB_2zip/",
    "/ayman-extract/partitions/partitions_1091MB_7/",
]

file_path = "profilers.json"

collection = JobCollection().load_from_file(file_path)


runtime_memories = [
    1769,
    # 3538,
    # 5308,
    # 7076,
    # 10240,
]
storage = Storage()
constant = 1


for path in input_data_paths:
    for mem in runtime_memories:
        parameters["RebinningStep"]["input_data_path"] = S3Path(path)
        chunk_size = storage.head_object(
            parameters["RebinningStep"]["input_data_path"].bucket,
            f"{parameters['RebinningStep']['input_data_path'].key}/partition_1.zip",
        )
        chunk_size = int(chunk_size["content-length"]) // MB
        print("Chunk size:", chunk_size)
        print("Runtime memory", mem)
        if chunk_size + constant > mem:
            print(
                f"Skipping run for memory {mem} MB and chunk size {chunk_size} MB (exceeds limit)"
            )
            continue

        collection = JobCollection().load_from_file(file_path)

        start_time = time.time()
        rebinning_profilers = RebinningStep(
            input_data_path=S3Path(parameters["RebinningStep"]["input_data_path"]),
            parameters=parameters["RebinningStep"]["parameters"],
            output=parameters["RebinningStep"]["output"],
        ).run(runtime_memory=mem)
        end_time = time.time()
        print(f"Rebinning took {end_time-start_time} seconds")
        collection.add_step_profiler(
            RebinningStep.__name__,
            mem,
            chunk_size,
            start_time,
            end_time,
            rebinning_profilers,
        )
        collection.save_to_file(file_path)
        plot_cost_vs_time_from_collection(collection, "rebinning/cost_vs_time")

        plot_gantt(
            collection,
            f"rebinning/gantt/chunk_size{chunk_size}",
            f"rebinning_gantt_runtime_{mem}.png",
            mem,
            chunk_size,
        )
        average_and_plot(
            collection,
            f"rebinning/runtime_stats/average/chunk_size{chunk_size}",
            f"rebinning_average_runtime_{mem}_chunksize_{chunk_size}.png",
            mem,
            chunk_size,
        )

    plot_cost_vs_time_pareto_real(
        collection, "rebinning/cost_vs_time_pareto_real", "RebinningStep", 1091
    )
