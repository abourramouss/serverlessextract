from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import imaging, monitor_and_run_imaging
from s3path import S3Path
from util import ProfilerCollection
from lithops import Storage
from util import CPUMetric, MemoryMetric, DiskMetric, NetworkMetric
import os

parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_30zip"),
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
print(
    parameters["RebinningStep"]["input_data_path"].bucket,
    parameters["RebinningStep"]["input_data_path"].key,
)
runtime_memory = 1768
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

print(collection.to_dict())

for step_profiler in collection:
    for profiler in step_profiler:
        print("------------------")
        for metric in profiler:
            print(metric)
        print("-------------------")


import matplotlib.pyplot as plt
from dataclasses import fields
import os


def aggregate_and_plot(collection, save_dir, filename):
    # Initialize dictionaries for aggregated metrics and timestamps
    aggregated_metrics = {}
    timestamps = {}

    # Collect and sum metrics by collection_id
    for step_profiler in collection:
        for profiler in step_profiler:
            for metric in profiler:
                cid = metric.collection_id

                # Sum the metrics for each collection_id
                if cid not in aggregated_metrics:
                    aggregated_metrics[cid] = {
                        "cpu_usage": 0,
                        "memory_usage": 0,
                        "disk_read_mb": 0,
                        "disk_write_mb": 0,
                        "net_read_mb": 0,
                        "net_write_mb": 0,
                    }
                    timestamps[cid] = []

                timestamps[cid].append(metric.timestamp)
                aggregated_metrics[cid]["cpu_usage"] += getattr(metric, "cpu_usage", 0)
                aggregated_metrics[cid]["memory_usage"] += getattr(
                    metric, "memory_usage", 0
                )
                aggregated_metrics[cid]["disk_read_mb"] += getattr(
                    metric, "disk_read_mb", 0
                )
                aggregated_metrics[cid]["disk_write_mb"] += getattr(
                    metric, "disk_write_mb", 0
                )
                aggregated_metrics[cid]["net_read_mb"] += getattr(
                    metric, "net_read_mb", 0
                )
                aggregated_metrics[cid]["net_write_mb"] += getattr(
                    metric, "net_write_mb", 0
                )

    # Convert timestamps to relative durations
    min_timestamp = min(min(ts_list) for ts_list in timestamps.values())
    duration = {
        cid: min(ts_list) - min_timestamp for cid, ts_list in timestamps.items()
    }

    # Prepare data for plotting
    sorted_cids = sorted(duration, key=duration.get)
    cpu_usages = [aggregated_metrics[cid]["cpu_usage"] for cid in sorted_cids]
    memory_usages = [aggregated_metrics[cid]["memory_usage"] for cid in sorted_cids]
    disk_read_mbs = [aggregated_metrics[cid]["disk_read_mb"] for cid in sorted_cids]
    disk_write_mbs = [aggregated_metrics[cid]["disk_write_mb"] for cid in sorted_cids]
    net_read_mbs = [aggregated_metrics[cid]["net_read_mb"] for cid in sorted_cids]
    net_write_mbs = [aggregated_metrics[cid]["net_write_mb"] for cid in sorted_cids]

    # Plotting
    plt.figure(figsize=(15, 10))
    plt.suptitle("Aggregated Profiler Metrics Over Relative Duration", fontsize=20)

    # Create subplots for each metric type
    plt.subplot(3, 2, 1)
    plt.plot(sorted_cids, cpu_usages, marker="o")
    plt.title("CPU Usage")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total CPU Usage (%)")

    plt.subplot(3, 2, 2)
    plt.plot(sorted_cids, memory_usages, marker="o")
    plt.title("Memory Usage")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total Memory Usage (MB)")

    plt.subplot(3, 2, 3)
    plt.plot(sorted_cids, disk_read_mbs, marker="o")
    plt.title("Disk Read")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total Disk Read (MB)")

    plt.subplot(3, 2, 4)
    plt.plot(sorted_cids, disk_write_mbs, marker="o")
    plt.title("Disk Write")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total Disk Write (MB)")

    plt.subplot(3, 2, 5)
    plt.plot(sorted_cids, net_read_mbs, marker="o")
    plt.title("Network Read")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total Network Read (MB)")

    plt.subplot(3, 2, 6)
    plt.plot(sorted_cids, net_write_mbs, marker="o")
    plt.title("Network Write")
    plt.xlabel("Relative Duration (seconds)")
    plt.ylabel("Total Network Write (MB)")

    plt.tight_layout()

    # Ensure the save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Save the plot
    plt.savefig(os.path.join(save_dir, filename))
    plt.close()


aggregate_and_plot(collection, "plots", "rebinning.png")
