from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubstractionStep, ApplyCalibrationStep
from steps.imaging import imaging, monitor_and_run_imaging
from s3path import S3Path
import logging
from util import setup_logging
from util import ProfilerPlotter
from util import ProfilerCollection
from lithops import Storage
import numpy as np
import pandas as pd
import os

logger = logging.getLogger(__name__)
setup_logging(logging.INFO)


parameters = {
    "RebinningStep": {
        "input_data_path": S3Path("/ayman-extract/partitions/partitions_61zip"),
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
print("Parent pid: ", os.getpid())
collection = ProfilerCollection()
# Instantiate the singleton Profilercollection
rebinning_profilers = RebinningStep(
    input_data_path=S3Path(parameters["RebinningStep"]["input_data_path"]),
    parameters=parameters["RebinningStep"]["parameters"],
    output=parameters["RebinningStep"]["output"],
).run(func_limit=1, runtime_memory=runtime_memory)

print(rebinning_profilers)

"""

Code to create a table of times for the different runtime and chunksizes, each cell represents the average time it took to execute rebinning for a specific runtime and chunksize
runtime_memories = [1769, 3538, 5308, 7076, 10240]  # runtime memory configurations
partition_sizes = [2]  # partition sizes
iterations = 3

tables = {
    "download_ms": {
        "mean": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
        "std": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
    },
    "execute_script": {
        "mean": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
        "std": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
    },
    "upload_rebinnedms": {
        "mean": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
        "std": pd.DataFrame(
            index=[7900 // p for p in partition_sizes], columns=runtime_memories
        ),
    },
}
# Collecting average durations and calculating mean and standard deviation
for i, p in enumerate(partition_sizes):
    input_size = 7900 // p
    for e in runtime_memories:
        average_durations = []  # Reset for each (input_size, e) combination
        operation_durations = {
            "download_ms": [],
            "execute_script": [],
            "upload_rebinnedms": [],
        }
        if input_size > e:
            continue
        for j in range(iterations):
            rebinning_profilers = RebinningStep(
                input_data_path=S3Path(f"/ayman-extract/partitions/partitions_{p}zip"),
                parameters=parameters["RebinningStep"]["parameters"],
                output=parameters["RebinningStep"]["output"],
            ).run(1, e)
            path = f"plots/reb/{7900//p}mb_{e}_{j}"
            ProfilerPlotter.plot_average_profiler(rebinning_profilers, path)
            ProfilerPlotter.plot_aggregated_profiler(rebinning_profilers, path)
            ProfilerPlotter.plot_aggregated_sum_profiler(rebinning_profilers, path)
            average_duration = ProfilerPlotter.plot_gantt(rebinning_profilers, path)
            average_durations.append(average_duration)

            for operation in operation_durations:
                operation_durations[operation].append(average_duration[operation])

        # Calculate mean and standard deviation for each operation and update tables
        for operation in operation_durations:
            mean = np.mean(operation_durations[operation])
            std = np.std(operation_durations[operation])
            tables[operation]["mean"].at[input_size, e] = mean
            tables[operation]["std"].at[input_size, e] = std


def update_or_append_row(path, sheet_name, dataframe, include_index=True):
    # Load the workbook and the specific sheet
    book = openpyxl.load_workbook(path)
    sheet = (
        book[sheet_name]
        if sheet_name in book.sheetnames
        else book.create_sheet(sheet_name)
    )

    # Convert dataframe to rows
    rows_gen = dataframe_to_rows(dataframe, index=include_index, header=False)

    # Iterate over the rows of the dataframe
    for df_row in rows_gen:
        # Skip the header row
        if df_row[0] == dataframe.index.name:
            continue

        input_size = df_row[0]  # Assuming this is the first element (index) in df_row
        row_updated = False

        # Iterate over the rows of the sheet starting from the second row
        for idx, sheet_row in enumerate(sheet.iter_rows(min_row=2), start=2):
            if sheet_row[0].value == input_size:
                # Update the existing row
                for col_idx, value in enumerate(df_row, start=1):
                    sheet.cell(row=idx, column=col_idx, value=value)
                row_updated = True
                break

        # Append a new row if not updated
        if not row_updated:
            sheet.append(df_row)

    # Save the workbook
    book.save(path)
    book.close()


excel_path = "time_stats.xlsx"
for operation in tables:
    mean_sheet_title = f"{operation} Mean"
    std_sheet_title = f"{operation} Std Dev"

    update_or_append_row(excel_path, mean_sheet_title, tables[operation]["mean"])
    update_or_append_row(excel_path, std_sheet_title, tables[operation]["std"])






"""


"""

rebinning_profilers = RebinningStep(
    input_data_path=S3Path(parameters["RebinningStep"]["input_data_path"]),
    parameters=parameters["RebinningStep"]["parameters"],
    output=parameters["RebinningStep"]["output"],
).run(1)

calibration_profilers = CalibrationStep(
    input_data_path=parameters["CalibrationStep"]["input_data_path"],
    parameters=parameters["CalibrationStep"]["parameters"],
    output=parameters["CalibrationStep"]["output"],
).run(1)

SubstractionStep(
    input_data_path=parameters["SubstractionStep"]["input_data_path"],
    parameters=parameters["SubstractionStep"]["parameters"],
    output=parameters["SubstractionStep"]["output"],
).run(1)


ApplyCalibrationStep(
    input_data_path=parameters["ApplyCalibrationStep"]["input_data_path"],
    parameters=parameters["ApplyCalibrationStep"]["parameters"],
    output=parameters["ApplyCalibrationStep"]["output"],
).run(1)


"""
