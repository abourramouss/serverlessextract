from typing import List
from executors import LithopsExecutor
from executors.executor import Executor
from steps.step import Step
from steps.rebinning import RebinningStep
from steps.calibration import CalibrationStep, SubtractionStep, ApplyCalibrationStep
from steps.imaging import ImagingStep
from datasource import LithopsDataSource
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import numpy as np


def generate_stats_df(stats_list, worker_ids):
    execution_times, io_times, io_sizes = [], [], []
    for i, stats in zip(worker_ids, stats_list):
        for step_name, step_data in stats.items():
            execution_times.append(
                {"worker": i, "step": step_name, "time": step_data.get("execution", 0)}
            )

            if "download_time" in step_data:
                io_times.append(
                    {
                        "worker": i,
                        "step": f"{step_name}_download",
                        "time": step_data["download_time"],
                    }
                )
                io_sizes.append(
                    {
                        "worker": i,
                        "step": f"{step_name}_download",
                        "size": step_data["download_size"],
                    }
                )

            if "upload_time" in step_data:
                io_times.append(
                    {
                        "worker": i,
                        "step": f"{step_name}_upload",
                        "time": step_data["upload_time"],
                    }
                )
                io_sizes.append(
                    {
                        "worker": i,
                        "step": f"{step_name}_upload",
                        "size": step_data["upload_size"],
                    }
                )

    return pd.DataFrame(execution_times), pd.DataFrame(io_times), pd.DataFrame(io_sizes)


def generate_step_df(stats, worker_id):
    steps, download_time, upload_time, download_size, upload_size, execution_time = (
        [],
        [],
        [],
        [],
        [],
        [],
    )

    for step_name, step_data in stats.items():
        steps.append(step_name)
        execution_time.append(step_data.get("execution", 0))
        download_time.append(step_data.get("download_time", 0))
        upload_time.append(step_data.get("upload_time", 0))
        download_size.append(step_data.get("download_size", 0))
        upload_size.append(step_data.get("upload_size", 0))

    return pd.DataFrame(
        {
            "Worker": [worker_id] * len(steps),
            "Step": steps,
            "Download Time": download_time,
            "Upload Time": upload_time,
            "Download Size": download_size,
            "Upload Size": upload_size,
            "Execution Time": execution_time,
        }
    )


def generate_plots(data_df, metric, statistic):
    data_df.plot(x="Step", y=statistic, kind="bar", legend=False)
    plt.ylabel(statistic)
    plt.title(f"{statistic} {metric} by Step")
    plt.tight_layout()
    plt.savefig(f"{statistic}_{metric}_by_step.png")


if "__main__" == __name__:
    # Pipeline parameters
    executor = LithopsExecutor()
    bucket_name = "aymanb-serverless-genomics"
    prefix = "extract-data/partitions_50/"
    output_dir = "/tmp/"
    extra_env = {"HOME": "/tmp"}
    extra_args = [bucket_name, output_dir]
    datasource = LithopsDataSource()
    all_keys = datasource.storage.list_keys(bucket_name, prefix)

    # Filter keys that include '.ms' in the directory name
    measurement_sets = [key for key in all_keys if ".ms" in key]
    measurement_sets = list(
        set("/".join(key.split("/")[:3]) for key in measurement_sets)
    )
    map = [
        RebinningStep(
            "extract-data/parameters/STEP1-flagrebin.parset", "rebinning.lua"
        ),
        CalibrationStep(
            "extract-data/parameters/STEP2A-calibration.parset",
            "extract-data/parameters/STEP2A-apparent.skymodel",
            "extract-data/parameters/apparent.sourcedb",
        ),
        SubtractionStep(
            "extract-data/parameters/STEP2B-subtract.parset",
            "extract-data/parameters/apparent.sourcedb",
        ),
        ApplyCalibrationStep("extract-data/parameters/STEP2C-applycal.parset"),
    ]

    reduce = ImagingStep(
        "extract-data/output/image",
    )
    # Execute all the steps that can be executed in parallel in a single worker.
    results_and_timings = executor.execute_steps(
        map, measurement_sets, extra_args=extra_args, extra_env=extra_env
    )

    # Generate list of result and stats
    calibrated_ms = [rt["result"] for rt in results_and_timings]
    stats_list = [rt["stats"] for rt in results_and_timings]

    # Execute Imaging step, Reduce phase.
    imaging = executor.execute_call_async(
        reduce, calibrated_ms, extra_args=extra_args, extra_env=extra_env
    )

    imaging_result = imaging["result"]
    imaging_stats = imaging["stats"]

    rebin_calib_stats = stats_list
    imaging_stats = {"Imaging": imaging_stats}

    print(rebin_calib_stats)
    print(imaging_stats)
    all_data_df = pd.concat(
        [
            generate_step_df(stats, worker_id)
            for worker_id, stats in enumerate(rebin_calib_stats)
        ]
        + [generate_step_df(imaging_stats, "Imaging")]
    )

    # Specify step order
    step_order = [
        "RebinningStep",
        "CalibrationStep",
        "SubtractionStep",
        "ApplyCalibrationStep",
        "Imaging",
    ]

    all_data_df["Step"] = pd.Categorical(
        all_data_df["Step"], categories=step_order, ordered=True
    )

    # Define metrics
    metrics = ["Execution Time", "I/O Time"]

    # Define statistics to compute for each metric
    statistics = ["mean", "min", "max"]

    for metric in metrics:
        all_data_df[metric] = (
            all_data_df["Download Time"] + all_data_df["Upload Time"]
            if metric == "I/O Time"
            else all_data_df["Execution Time"]
        )
        statistic_df = (
            all_data_df.groupby("Step")
            .agg({metric: ["mean", "min", "max"]})
            .reset_index()
        )

        # Plot the mean values with error bars
        x = np.arange(len(statistic_df))
        mean_values = statistic_df[(metric, "mean")]
        min_values = statistic_df[(metric, "min")]
        max_values = statistic_df[(metric, "max")]

        plt.bar(
            x,
            mean_values,
            yerr=[mean_values - min_values, max_values - mean_values],
            capsize=5,
        )
        plt.xticks(x, statistic_df["Step"])
        plt.ylabel(metric)
        plt.title(f"{metric} by Step")
        plt.tight_layout()

        # Save the plot to a file, replacing '/' with '_'
        safe_metric = metric.replace("/", "_")
        plt.savefig(f"{safe_metric}_by_step.png")

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 15))  # adjust the size as needed

    # Define color for each phase
    colors = {"download_time": "blue", "execution": "orange", "upload_time": "green"}
    labels = {
        "download_time": "Download Time",
        "execution": "Execution Time",
        "upload_time": "Upload Time",
    }

    # Steps order
    steps_order = [
        "RebinningStep",
        "CalibrationStep",
        "SubtractionStep",
        "ApplyCalibrationStep",
    ]

    # Order the results_and_timings and rebin_calib_stats by start_time
    results_and_timings = sorted(results_and_timings, key=lambda x: x["start_time"])
    rebin_calib_stats = [rt["stats"] for rt in results_and_timings]

    # Calculate the minimum start time
    min_start_time = results_and_timings[0]["start_time"]

    # Update the Gantt chart code to include start times
    for i, worker in enumerate(rebin_calib_stats):
        # Initialize start time
        start_time = results_and_timings[i]["start_time"] - min_start_time
        for step in steps_order:
            # Check if the step exists in worker data
            if step in worker:
                for phase, color in colors.items():
                    # Check if the phase exists in this step
                    if phase in worker[step]:
                        duration = worker[step][phase]
                        ax.broken_barh(
                            [(start_time, duration)], (i - 0.4, 0.8), facecolors=color
                        )
                        ax.text(
                            start_time + duration / 2,
                            i,
                            round(duration, 2),
                            ha="center",
                            va="center",
                            color="white",
                        )
                        start_time += duration  # Update start time for next phase

    # Set labels and title
    ax.set_xlabel("Time (seconds)")
    ax.set_yticks(range(len(rebin_calib_stats)))
    ax.set_yticklabels([f"Worker {i}" for i in range(len(rebin_calib_stats))])
    ax.set_title("Gantt Chart of Download, Execution, and Upload Times for Each Step")

    # Create a legend
    patches = [
        mpatches.Patch(color=color, label=label)
        for phase, color in colors.items()
        for label in labels.items()
        if phase == label[0]
    ]
    ax.legend(handles=patches)

    plt.tight_layout()

    # Save the figure
    plt.savefig("gantt_chart_steps.png", dpi=300)
    # Save table as CSV
    statistic_df.to_csv("steps_statistics.csv", index=False)
