import matplotlib.pyplot as plt
import os
import numpy as np
import pandas as pd


def aggregate_and_plot(collection, save_dir, filename):
    aggregated_metrics = {}
    timestamps = {}

    for step_profiler in collection:
        for profiler in step_profiler:
            for metric in profiler:
                cid = metric.collection_id
                if cid not in aggregated_metrics:
                    aggregated_metrics[cid] = {
                        "cpu_usage": 0,
                        "memory_usage": 0,
                        "disk_read_mb": 0,
                        "disk_write_mb": 0,
                        "net_read_mb": 0,
                        "net_write_mb": 0,
                    }
                timestamps.setdefault(cid, []).append(metric.timestamp)
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
                aggregated_metrics[cid]["net_read_mb"] = getattr(
                    metric, "net_read_mb", 0
                )
                aggregated_metrics[cid]["net_write_mb"] = getattr(
                    metric, "net_write_mb", 0
                )

    min_timestamp = min(min(ts) for ts in timestamps.values())
    relative_times = [ts[0] - min_timestamp for ts in sorted(timestamps.values())]

    cpu_usages = [metrics["cpu_usage"] for metrics in aggregated_metrics.values()]
    memory_usages = [metrics["memory_usage"] for metrics in aggregated_metrics.values()]
    disk_read_rates, disk_write_rates, net_read_rates, net_write_rates = [], [], [], []

    sorted_cids = sorted(aggregated_metrics)
    for i, cid in enumerate(sorted_cids):
        if i == 0:
            disk_read_rates.append(0)
            disk_write_rates.append(0)
            net_read_rates.append(0)
            net_write_rates.append(0)
            continue

        prev_cid = sorted_cids[i - 1]
        time_diff = max(relative_times[i] - relative_times[i - 1], 1)

        for metric, rate_list in [
            ("disk_read_mb", disk_read_rates),
            ("disk_write_mb", disk_write_rates),
        ]:
            rate = (
                aggregated_metrics[cid][metric] - aggregated_metrics[prev_cid][metric]
            ) / time_diff
            rate_list.append(max(rate, 0))

        for metric, rate_list in [
            ("net_read_mb", net_read_rates),
            ("net_write_mb", net_write_rates),
        ]:
            rate = (
                aggregated_metrics[cid][metric] - aggregated_metrics[prev_cid][metric]
            ) / time_diff
            rate_list.append(max(rate, 0))

    plt.figure(figsize=(15, 10))
    plt.suptitle("Aggregated Profiler Metrics Over Relative Duration", fontsize=20)

    plt.subplot(3, 2, 1)
    plt.plot(relative_times, cpu_usages, marker="o")
    plt.title("CPU Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("CPU Usage (%)")

    plt.subplot(3, 2, 2)
    plt.plot(relative_times, memory_usages, marker="o")
    plt.title("Memory Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory Usage (MB)")

    for i, (title, data) in enumerate(
        [
            ("Disk Read Rate", disk_read_rates),
            ("Disk Write Rate", disk_write_rates),
            ("Network Read Rate", net_read_rates),
            ("Network Write Rate", net_write_rates),
        ],
        start=3,
    ):
        plt.subplot(3, 2, i)
        plt.plot(relative_times, data, marker="o")
        plt.title(title)
        plt.xlabel("Time (s)")
        plt.ylabel(f"{title} (MB/s)")

    plt.tight_layout()
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, filename))
    plt.close()


def plot_gantt(collection, save_dir):
    figure_height = len(collection) * 2
    fig, ax = plt.subplots(figsize=(10, figure_height))

    colors = plt.cm.Pastel1.colors

    min_start_time = min(
        timer.start_time
        for step_profiler in collection
        for profiler in step_profiler.profilers
        for timer in profiler.function_timers
    )

    y_position = 0
    for step_profiler in collection:
        for profiler in step_profiler.profilers:
            for idx, timer in enumerate(profiler.function_timers):
                color_idx = idx % len(colors)
                relative_start_time = timer.start_time - min_start_time
                ax.broken_barh(
                    [(relative_start_time, timer.duration)],
                    (y_position - 0.4, 0.8),
                    facecolors=colors[color_idx],
                )
                y_position += 1

    ax.set_yticks([0.5 + i for i in range(y_position)])
    ax.set_yticklabels(
        [
            f"{profiler.function_timers[i].label} (Profiler {p_idx + 1})"
            for p_idx, profiler in enumerate(step_profiler.profilers)
            for i in range(len(profiler.function_timers))
        ]
    )
    ax.set_ylim(-1, y_position)

    ax.set_xlabel("Time (s) since Start")
    ax.set_ylabel("Function Timers")
    ax.set_title("Gantt Chart of Function Timers")

    plt.subplots_adjust(left=0.2, bottom=0.2, right=0.75)

    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, "gantt.png")
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches="tight")
    print(f"Plot saved to: {save_path}")


def average_and_plot(collection, save_dir, filename):
    average_metrics = {}
    timestamps = {}

    for step_profiler in collection:
        for profiler in step_profiler:
            for metric in profiler:
                cid = metric.collection_id
                if cid not in average_metrics:
                    average_metrics[cid] = {
                        "cpu_usage": 0,
                        "memory_usage": 0,
                        "disk_read_mb": 0,
                        "disk_write_mb": 0,
                        "net_read_mb": 0,
                        "net_write_mb": 0,
                    }
                timestamps.setdefault(cid, []).append(metric.timestamp)
                average_metrics[cid]["cpu_usage"] += getattr(metric, "cpu_usage", 0)
                average_metrics[cid]["memory_usage"] += getattr(
                    metric, "memory_usage", 0
                )
                average_metrics[cid]["disk_read_mb"] += getattr(
                    metric, "disk_read_mb", 0
                )
                average_metrics[cid]["disk_write_mb"] += getattr(
                    metric, "disk_write_mb", 0
                )
                average_metrics[cid]["net_read_mb"] += getattr(metric, "net_read_mb", 0)
                average_metrics[cid]["net_write_mb"] += getattr(
                    metric, "net_write_mb", 0
                )

    min_timestamp = min(min(ts) for ts in timestamps.values())
    relative_times = [ts[0] - min_timestamp for ts in sorted(timestamps.values())]

    average_cpu_usages = [
        metrics["cpu_usage"] / len(step_profiler)
        for metrics, step_profiler in zip(average_metrics.values(), collection)
    ]
    average_memory_usages = [
        metrics["memory_usage"] / len(step_profiler)
        for metrics, step_profiler in zip(average_metrics.values(), collection)
    ]
    (
        average_disk_read_rates,
        average_disk_write_rates,
        average_net_read_rates,
        average_net_write_rates,
    ) = ([], [], [], [])

    sorted_cids = sorted(average_metrics)
    for i, cid in enumerate(sorted_cids):
        if i == 0:
            average_disk_read_rates.append(0)
            average_disk_write_rates.append(0)
            average_net_read_rates.append(0)
            average_net_write_rates.append(0)
            continue

        prev_cid = sorted_cids[i - 1]
        time_diff = max(relative_times[i] - relative_times[i - 1], 1)

        for metric, rate_list in [
            ("disk_read_mb", average_disk_read_rates),
            ("disk_write_mb", average_disk_write_rates),
        ]:
            rate = (
                (average_metrics[cid][metric] - average_metrics[prev_cid][metric])
                / time_diff
                / len(step_profiler)
            )
            rate_list.append(max(rate, 0))

        for metric, rate_list in [
            ("net_read_mb", average_net_read_rates),
            ("net_write_mb", average_net_write_rates),
        ]:
            rate = (
                (average_metrics[cid][metric] - average_metrics[prev_cid][metric])
                / time_diff
                / len(step_profiler)
            )
            rate_list.append(max(rate, 0))

    plt.figure(figsize=(15, 10))
    plt.suptitle("Averaged Profiler Metrics Over Relative Duration", fontsize=20)

    min_len = min(len(relative_times), len(average_cpu_usages))
    relative_times = relative_times[:min_len]
    average_cpu_usages = average_cpu_usages[:min_len]

    plt.subplot(3, 2, 1)
    plt.plot(relative_times, average_cpu_usages, marker="o")
    plt.title("Average CPU Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("CPU Usage (%)")

    plt.subplot(3, 2, 2)
    plt.plot(relative_times, average_memory_usages, marker="o")
    plt.title("Average Memory Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory Usage (MB)")

    for i, (title, data) in enumerate(
        [
            ("Average Disk Read Rate", average_disk_read_rates),
            ("Average Disk Write Rate", average_disk_write_rates),
            ("Average Network Read Rate", average_net_read_rates),
            ("Average Network Write Rate", average_net_write_rates),
        ],
        start=3,
    ):
        plt.subplot(3, 2, i)
        plt.plot(relative_times, data, marker="o")
        plt.title(title)
        plt.xlabel("Time (s)")
        plt.ylabel(f"{title} (MB/s)")

    plt.tight_layout()
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, filename))
    plt.close()


def average_and_plot(collection, save_dir, filename):
    aggregated_metrics = {}
    timestamps = {}

    for step_profiler in collection:
        profiler_count = len(step_profiler)
        for profiler in step_profiler:
            for metric in profiler:
                cid = metric.collection_id
                if cid not in aggregated_metrics:
                    aggregated_metrics[cid] = {
                        "cpu_usage": 0,
                        "memory_usage": 0,
                        "disk_read_mb": 0,
                        "disk_write_mb": 0,
                        "net_read_mb": 0,
                        "net_write_mb": 0,
                    }
                timestamps.setdefault(cid, []).append(metric.timestamp)
                for key in aggregated_metrics[cid]:
                    value = getattr(metric, key, 0)
                    aggregated_metrics[cid][key] += value

        for cid in aggregated_metrics:
            for key in aggregated_metrics[cid]:
                aggregated_metrics[cid][key] /= profiler_count

    min_timestamp = min(min(ts) for ts in timestamps.values())
    relative_times = [ts[0] - min_timestamp for ts in sorted(timestamps.values())]
    cpu_usages = [metrics["cpu_usage"] for metrics in aggregated_metrics.values()]
    memory_usages = [metrics["memory_usage"] for metrics in aggregated_metrics.values()]
    disk_read_rates, disk_write_rates, net_read_rates, net_write_rates = [], [], [], []

    sorted_cids = sorted(aggregated_metrics)
    for i, cid in enumerate(sorted_cids):
        if i == 0:
            disk_read_rates.append(0)
            disk_write_rates.append(0)
            net_read_rates.append(0)
            net_write_rates.append(0)
            continue

        prev_cid = sorted_cids[i - 1]
        time_diff = max(relative_times[i] - relative_times[i - 1], 1)

        for metric, rate_list in [
            ("disk_read_mb", disk_read_rates),
            ("disk_write_mb", disk_write_rates),
        ]:
            rate = (
                aggregated_metrics[cid][metric] - aggregated_metrics[prev_cid][metric]
            ) / time_diff
            rate_list.append(max(rate, 0))

        for metric, rate_list in [
            ("net_read_mb", net_read_rates),
            ("net_write_mb", net_write_rates),
        ]:
            rate = (
                aggregated_metrics[cid][metric] - aggregated_metrics[prev_cid][metric]
            ) / time_diff
            rate_list.append(max(rate, 0))

    plt.figure(figsize=(15, 10))
    plt.suptitle("Average Profiler Metrics Over Relative Duration", fontsize=20)

    plt.subplot(3, 2, 1)
    plt.plot(relative_times, cpu_usages, marker="o")
    plt.title("Average CPU Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("CPU Usage (%)")

    plt.subplot(3, 2, 2)
    plt.plot(relative_times, memory_usages, marker="o")
    plt.title("Average Memory Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory Usage (MB)")

    for i, (title, data) in enumerate(
        [
            ("Average Disk Read Rate", disk_read_rates),
            ("Average Disk Write Rate", disk_write_rates),
            ("Average Network Read Rate", net_read_rates),
            ("Average Network Write Rate", net_write_rates),
        ],
        start=3,
    ):
        plt.subplot(3, 2, i)
        plt.plot(relative_times, data, marker="o")
        plt.title(title)
        plt.xlabel("Time (s)")
        plt.ylabel(f"{title} (MB/s)")

    plt.tight_layout()
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, filename))
    plt.close()
