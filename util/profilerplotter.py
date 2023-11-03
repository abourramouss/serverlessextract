import numpy as np
import matplotlib.pyplot as plt
import os
from itertools import cycle


class ProfilerPlotter:
    @staticmethod
    def resample_worker_data(original_times, data, bins):
        """Resample worker data into fixed bins."""
        bin_indices = np.digitize(original_times, bins)
        resampled_data = []

        for i in range(len(bins) - 1):
            values_in_bin = [
                data[j]
                for j, bin_idx in enumerate(bin_indices)
                if bin_idx == i and j < len(data)
            ]
            if values_in_bin:
                resampled_data.append(np.mean(values_in_bin))
            else:
                resampled_data.append(
                    np.nan
                )  # or forward fill: resampled_data.append(resampled_data[-1])

        return resampled_data

    @staticmethod
    def average(profilers):
        # Flatten the list of all timestamps
        all_timestamps = [
            time for profiler in profilers for time in profiler.timestamps
        ]

        # Determine the overall time frame
        min_time = min(all_timestamps)
        max_time = max(all_timestamps)

        # Create bins (e.g., every second)
        bin_size = 1  # Adjust this based on your needs
        bins = np.arange(min_time, max_time + bin_size, bin_size)

        # Resample data for all profilers
        cpu_percent_data = []

        for profiler in profilers:
            resampled_data = ProfilerPlotter.resample_worker_data(
                profiler.timestamps, profiler.cpu_percent, bins
            )
            cpu_percent_data.append(resampled_data)

        # Compute average for each bin
        avg_cpu_percent = np.nanmean(cpu_percent_data, axis=0)

        # Return bin centers as the representative time for each interval
        bin_centers = (bins[:-1] + bins[1:]) / 2

        return bin_centers, avg_cpu_percent

    @staticmethod
    def plot_average_profiler(profilers, save_dir):
        # Calculate the maximum duration across all profilers
        max_duration = 0
        for profiler in profilers:
            timestamps_array = np.array(profiler.timestamps)
            relative_duration = timestamps_array[-1] - timestamps_array[0]
            if relative_duration > max_duration:
                max_duration = relative_duration

        # Interpolate all profiler data onto a common timestamp array based on max_duration
        common_timestamps = np.linspace(0, max_duration, 1000)

        metrics = [
            ("CPU Usage (%)", [profiler.cpu_percent for profiler in profilers], "b"),
            (
                "Memory Used (MB)",
                [profiler.memory_used_mb for profiler in profilers],
                "g",
            ),
            (
                "Disk Read Rate (MB/s)",
                [profiler.disk_read_rate_mb for profiler in profilers],
                "r",
            ),
            (
                "Disk Write Rate (MB/s)",
                [profiler.disk_write_rate_mb for profiler in profilers],
                "c",
            ),
            ("MB sent (MB/s)", [profiler.bytes_sent for profiler in profilers], "m"),
            (
                "MB recieved (MB/s)",
                [profiler.bytes_recv for profiler in profilers],
                "y",
            ),
        ]

        plt.figure(figsize=(20, 30))
        plt.suptitle("Profiler Metrics Averages", fontsize=20, y=1.03)

        for index, (label, metric_data, color) in enumerate(metrics):
            summed_metric_data = np.zeros_like(common_timestamps)

            for profiler, metric in zip(profilers, metric_data):
                # Convert absolute timestamps to relative timestamps
                timestamps_array = np.array(profiler.timestamps)
                relative_timestamps = timestamps_array - timestamps_array[0]

                # Ensure that the lengths of relative_timestamps and metric match
                min_length = min(len(relative_timestamps), len(metric))
                relative_timestamps = relative_timestamps[:min_length]
                metric = metric[:min_length]

                interpolated_metric = np.interp(
                    common_timestamps, relative_timestamps, metric
                )
                summed_metric_data += interpolated_metric

            average_metric_data = summed_metric_data / len(profilers)

            ax = plt.subplot(3, 2, index + 1)
            ax.plot(common_timestamps, average_metric_data, color=color, label=label)
            ax.set_title(label, fontsize=16)
            ax.set_xlabel("Time (seconds since start)", fontsize=14)
            ax.set_ylabel(label, fontsize=14)
            ax.grid(True, which="both", linestyle="--", linewidth=0.5)
            ax.legend(loc="upper left")

        # Adjust layout
        plt.tight_layout()

        # Ensure the directory exists
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # Save the plot
        save_path = os.path.join(save_dir, "combined_metrics.png")
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Plot saved to: {save_path}")

    @staticmethod
    def plot_aggregated_profiler(profilers, save_dir):
        # Define a list of colors to cycle through
        colors = cycle(["b", "g", "r", "c", "m", "y", "k"])

        # Metrics to be plotted without color codes
        metrics = [
            ("CPU Usage (%)", [profiler.cpu_percent for profiler in profilers]),
            ("Memory Used (MB)", [profiler.memory_used_mb for profiler in profilers]),
            (
                "Disk Read Rate (MB/s)",
                [profiler.disk_read_rate_mb for profiler in profilers],
            ),
            (
                "Disk Write Rate (MB/s)",
                [profiler.disk_write_rate_mb for profiler in profilers],
            ),
            ("MB sent (MB/s)", [profiler.bytes_sent for profiler in profilers]),
            ("MB received (MB/s)", [profiler.bytes_recv for profiler in profilers]),
        ]

        plt.figure(figsize=(20, 30))
        plt.suptitle("Aggregated Resource Usage", fontsize=20, y=1.03)

        for index, (label, metric_data) in enumerate(metrics):
            ax = plt.subplot(3, 2, index + 1)

            # Find the smallest timestamp among all profilers
            min_timestamp = min([min(profiler.timestamps) for profiler in profilers])

            # For each profiler, plot the metric data over its relative timestamps
            for profiler, metric in zip(profilers, metric_data):
                timestamps_array = np.array(profiler.timestamps) - min_timestamp
                min_length = min(len(timestamps_array), len(metric))
                timestamps_array = timestamps_array[:min_length]
                metric = metric[:min_length]

                # Get the next color from the cycle
                color = next(colors)

                ax.plot(
                    timestamps_array,
                    metric,
                    color=color,
                    label=f"{label} (Worker {profilers.index(profiler)+1})",
                )

            ax.set_title(label, fontsize=16)
            ax.set_xlabel("Time (relative)", fontsize=14)
            ax.set_ylabel(label, fontsize=14)
            ax.grid(True, which="both", linestyle="--", linewidth=0.5)
            ax.legend(loc="upper left")

        # Adjust layout
        plt.tight_layout()

        # Ensure the directory exists
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # Save the plot
        save_path = os.path.join(save_dir, "aggregated_metrics.png")
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Plot saved to: {save_path}")

    @staticmethod
    def plot_aggregated_sum_profiler(profilers, save_dir):
        # Calculate the maximum duration across all profilers
        max_duration = 0
        for profiler in profilers:
            timestamps_array = np.array(profiler.timestamps)
            relative_duration = timestamps_array[-1] - timestamps_array[0]
            if relative_duration > max_duration:
                max_duration = relative_duration

        # Interpolate all profiler data onto a common timestamp array based on max_duration
        common_timestamps = np.linspace(0, max_duration, 1000)

        metrics = [
            ("CPU Usage (%)", [profiler.cpu_percent for profiler in profilers]),
            ("Memory Used (MB)", [profiler.memory_used_mb for profiler in profilers]),
            (
                "Disk Read Rate (MB/s)",
                [profiler.disk_read_rate_mb for profiler in profilers],
            ),
            (
                "Disk Write Rate (MB/s)",
                [profiler.disk_write_rate_mb for profiler in profilers],
            ),
            ("MB sent (MB/s)", [profiler.bytes_sent for profiler in profilers]),
            ("MB received (MB/s)", [profiler.bytes_recv for profiler in profilers]),
        ]

        plt.figure(figsize=(20, 30))
        plt.suptitle("Aggregated Resource Usage", fontsize=20, y=1.03)

        for index, (label, metric_data) in enumerate(metrics):
            summed_metric_data = np.zeros_like(common_timestamps)

            for profiler, metric in zip(profilers, metric_data):
                # Convert absolute timestamps to relative timestamps
                timestamps_array = np.array(profiler.timestamps)
                relative_timestamps = timestamps_array - timestamps_array[0]

                # Ensure that the lengths of relative_timestamps and metric match
                min_length = min(len(relative_timestamps), len(metric))
                relative_timestamps = relative_timestamps[:min_length]
                metric = metric[:min_length]

                interpolated_metric = np.interp(
                    common_timestamps, relative_timestamps, metric
                )
                summed_metric_data += interpolated_metric

            ax = plt.subplot(3, 2, index + 1)
            ax.plot(common_timestamps, summed_metric_data, label=f"Total {label}")
            ax.set_title(f"Total {label}", fontsize=16)
            ax.set_xlabel("Time (seconds since start)", fontsize=14)
            ax.set_ylabel(label, fontsize=14)
            ax.grid(True, which="both", linestyle="--", linewidth=0.5)
            ax.legend(loc="upper left")

        # Adjust layout
        plt.tight_layout()

        # Ensure the directory exists
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # Save the plot
        save_path = os.path.join(save_dir, "aggregated_total_metrics.png")
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Plot saved to: {save_path}")

    @staticmethod
    def plot_gantt(profilers, save_dir):
        fig, ax = plt.subplots(figsize=(10, len(profilers) + 2))

        # Sort profilers based on start time, 0 has the lowest start time
        profilers = sorted(profilers, key=lambda p: p.time_records[0]["start_time"])

        pastel_colors = [
            "#a8e6cf",
            "#dcedc1",
            "#ffd3b6",
            "#ffaaa5",
            "#dcedc2",
            "#a8e6ce",
        ]

        for idx, profiler in enumerate(profilers):
            previous_end_time = 0  # This will hold the end time of the previous task

            for i, record in enumerate(profiler.time_records):
                # If it's not the first record, adjust the relative start time based on the previous task's end time
                if i > 0:
                    relative_start_time = (
                        previous_end_time - profilers[0].time_records[0]["start_time"]
                    )
                else:
                    relative_start_time = (
                        record["start_time"]
                        - profilers[0].time_records[0]["start_time"]
                    )

                previous_end_time = record["end_time"]

                duration = record["duration"]
                ax.barh(
                    idx,
                    duration,
                    left=relative_start_time,
                    color=pastel_colors[i % len(pastel_colors)],
                    edgecolor="white",
                    height=0.5,
                )

                segment_center = relative_start_time + duration / 2
                ax.text(
                    segment_center,
                    idx,
                    f"{record['label']}\n{duration:.2f}s",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="black",
                )

        ax.set_yticks(range(len(profilers)))
        ax.set_yticklabels([f"Worker {i+1}" for i in range(len(profilers))])
        ax.set_ylim(-1, len(profilers))
        ax.set_xlabel("Execution Time (s)")
        ax.set_ylabel("Profiler (Workers)")
        ax.set_title("Gantt plot")
        ax.legend(
            [rec["label"] for rec in profilers[0].time_records], loc="upper right"
        )

        plt.tight_layout()
        # Ensure the directory exists
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # Save the plot
        save_path = os.path.join(save_dir, "gantt.png")
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Plot saved to: {save_path}")
