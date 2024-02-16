import os
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
import math
import json
from adjustText import adjust_text
from collections import defaultdict


def aggregate_and_plot(
    collection, save_dir, filename, specified_memory, specified_chunk_size
):
    aggregated_metrics = {}
    timestamps = {}

    for step_profiler in collection:
        if (
            step_profiler.memory == specified_memory
            and step_profiler.chunk_size == specified_chunk_size
        ):
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
                    aggregated_metrics[cid]["cpu_usage"] += getattr(
                        metric, "cpu_usage", 0
                    )
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

    if not aggregated_metrics:
        print("No matching data found for the specified memory and chunk size.")
        return

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


def average_and_plot(
    job_collection, save_dir, filename, specified_memory, specified_chunk_size
):
    aggregated_metrics = {}
    timestamps = {}
    profiler_count = 0

    for step_name, job in job_collection:
        if job.memory == specified_memory and job.chunk_size == specified_chunk_size:
            for profiler in job.profilers:
                profiler_count += 1
                for (
                    metric
                ) in profiler.metrics:  # Assuming profiler has a 'metrics' attribute
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

    if profiler_count > 1:
        for cid in aggregated_metrics:
            for key in aggregated_metrics[cid]:
                aggregated_metrics[cid][key] /= profiler_count

    if not aggregated_metrics:
        print("No matching data found for the specified memory and chunk size.")
        return

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
    print(f"Plot saved to: {os.path.join(save_dir, filename)}")


def plot_gantt(
    job_collection, save_dir, filename, specified_memory, specified_chunk_size
):
    timer_data = defaultdict(
        lambda: {"total_start": 0, "total_duration": 0, "count": 0}
    )

    for step_name, job in job_collection:
        if job.memory == specified_memory and job.chunk_size == specified_chunk_size:
            for profiler in job.profilers:
                for timer in profiler.function_timers:
                    data = timer_data[timer.label]
                    data["total_start"] += timer.start_time
                    data["total_duration"] += timer.duration
                    data["count"] += 1

    # Averaging the data
    for label, data in timer_data.items():
        if data["count"] > 0:
            data["avg_start"] = data["total_start"] / data["count"]
            data["avg_duration"] = data["total_duration"] / data["count"]

    if not timer_data:
        print("No matching data found for the specified memory and chunk size.")
        return

    # Find the earliest average start time
    min_start_time = min(data["avg_start"] for data in timer_data.values())

    # Set global font size
    plt.rcParams.update({"font.size": 18})

    # Plotting
    figure_height = len(timer_data) * 2
    fig, ax = plt.subplots(figsize=(10, figure_height))
    colors = plt.cm.Pastel1.colors

    for idx, (label, data) in enumerate(timer_data.items()):
        color_idx = idx % len(colors)
        relative_start_time = data["avg_start"] - min_start_time
        ax.broken_barh(
            [(relative_start_time, data["avg_duration"])],
            (idx - 0.4, 0.8),
            facecolors=colors[color_idx],
        )
        text_x = relative_start_time + data["avg_duration"] / 2
        text_y = idx
        ax.text(
            text_x,
            text_y,
            f"{data['avg_duration']:.2f}s",
            va="center",
            ha="center",
            fontsize=18,
        )

    ax.set_yticks([0.5 + i for i in range(len(timer_data))])
    ax.set_yticklabels([label for label in timer_data], fontsize=18)
    ax.tick_params(axis="x", labelsize=18)
    ax.set_ylim(-1, len(timer_data))
    ax.set_xlabel("Average Time (s) since Start", fontsize=18)
    ax.set_ylabel("Function Timers", fontsize=18)
    ax.set_title("Gantt Chart of Averaged Function Timers", fontsize=18)

    plt.subplots_adjust(left=0.2, bottom=0.2, right=0.75)
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, filename)
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches="tight")
    plt.close()
    print(f"Plot saved to: {save_path}")


def normalize(data):
    """Normalize the data to the range [0, 1]."""
    min_val = np.min(data)
    max_val = np.max(data)
    return (data - min_val) / (max_val - min_val)


def calculate_distance_to_origin(cost, time):
    """Calculate Euclidean distance from the origin."""
    return np.sqrt(cost**2 + time**2)


def is_pareto_efficient(costs):
    is_efficient = np.ones(costs.shape[0], dtype=bool)
    for i, c in enumerate(costs):
        if is_efficient[i]:
            is_efficient[is_efficient] = np.any(costs[is_efficient] < c, axis=1)
            is_efficient[i] = True
    return is_efficient


def find_pareto(costs, times, details):
    """Identify Pareto optimal points (cost, time) along with their details."""
    paired_points = list(zip(costs, times, details))
    pareto_points = []
    for point in paired_points:
        if not any(
            (other[0] <= point[0] and other[1] < point[1]) for other in paired_points
        ):
            pareto_points.append(point)
    pareto_costs, pareto_times, pareto_details = zip(*pareto_points)
    return list(pareto_costs), list(pareto_times), list(pareto_details)


def plot_cost_vs_time_from_collection(job_collection, save_dir):
    cost_per_ms_per_mb = 0.0000000167
    averaged_profilers = defaultdict(lambda: defaultdict(list))

    # Process the collection data
    for step_name, job in job_collection:
        for profiler in job.profilers:
            total_time_worker = (
                profiler.worker_end_tstamp - profiler.worker_start_tstamp
            )
            cost = total_time_worker * 1000 * cost_per_ms_per_mb * (job.memory / 1024)
            key = (job.memory, job.chunk_size)
            averaged_profilers[key]["times"].append(total_time_worker)
            averaged_profilers[key]["costs"].append(cost)

    # Average the data
    averaged_data = defaultdict(dict)
    for (memory, chunk_size), values in averaged_profilers.items():
        average_time = sum(values["times"]) / len(values["times"])
        average_cost = sum(values["costs"]) / len(values["costs"])
        averaged_data[chunk_size][memory] = (average_time, average_cost)

    # Setup the plot
    plt.figure(figsize=(15, 10))
    plt.title(
        "Cost vs Execution Time for Various Chunk Sizes and Memory Configurations",
        fontsize=18,
    )

    unique_chunk_sizes = set(cs for _, cs in averaged_profilers.keys())
    colors = plt.cm.rainbow(np.linspace(0, 1, len(unique_chunk_sizes)))
    color_map = {
        chunk_size: color for chunk_size, color in zip(unique_chunk_sizes, colors)
    }

    for chunk_size, memory_data in averaged_data.items():
        times, costs, memories = zip(
            *[(time, cost, mem) for mem, (time, cost) in memory_data.items()]
        )
        normalized_times = normalize(np.array(times))
        normalized_costs = normalize(np.array(costs))

        # Calculate distances using normalized values and find the minimum distance point
        distances = np.array(
            [
                calculate_distance_to_origin(nc, nt)
                for nc, nt in zip(normalized_costs, normalized_times)
            ]
        )
        min_distance_idx = np.argmin(distances)
        optimal_time = times[min_distance_idx]
        optimal_cost = costs[min_distance_idx]
        optimal_memory = memories[min_distance_idx]

        min_time_idx = np.argmin(times)
        max_time_idx = np.argmax(times)

        # Assign the color for the current chunk size
        color = color_map[chunk_size]

        plt.scatter(times, costs, color=color, s=50, label=f"{chunk_size} MB Chunk")
        plt.plot(times, costs, color=color)  # Connect points of the same chunk size

        # Highlight and label the optimal, maximum, and minimum points
        plt.scatter(
            [optimal_time],
            [optimal_cost],
            color=color,
            edgecolor="black",
            marker="D",
            s=100,
        )
        plt.scatter(
            [times[max_time_idx]],
            [costs[max_time_idx]],
            color=color,
            edgecolor="black",
            marker="X",
            s=100,
        )
        plt.scatter(
            [times[min_time_idx]],
            [costs[min_time_idx]],
            color=color,
            edgecolor="black",
            marker="X",
            s=100,
        )
        plt.text(
            optimal_time,
            optimal_cost,
            f"Optimal: {optimal_memory} MB",
            fontsize=14,
            ha="right",
            va="bottom",
        )
        plt.text(
            times[max_time_idx],
            costs[max_time_idx],
            f"Max: {memories[max_time_idx]} MB",
            fontsize=14,
            ha="right",
            va="bottom",
        )
        plt.text(
            times[min_time_idx],
            costs[min_time_idx],
            f"Min: {memories[min_time_idx]} MB",
            fontsize=14,
            ha="right",
            va="bottom",
        )

    plt.xlabel("Execution Time (seconds)", fontsize=16)
    plt.ylabel("Cost", fontsize=16)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.legend(fontsize=14)
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "averaged_cost_vs_time_job_collection.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Plot saved to: {save_path}")


def calculate_speedup(baseline_time, current_time):
    return baseline_time / current_time if current_time else 0


def plot_speedup_vs_cost_from_collection(job_collection, save_dir):
    cost_per_ms_per_mb = 0.0000000167
    averaged_profilers = defaultdict(lambda: defaultdict(list))

    # Process the collection data
    for step_name, job in job_collection:
        for profiler in job.profilers:
            total_time_worker = (
                profiler.worker_end_tstamp - profiler.worker_start_tstamp
            )
            cost = total_time_worker * 1000 * cost_per_ms_per_mb * (job.memory / 1024)
            key = (job.memory, job.chunk_size)
            averaged_profilers[key]["times"].append(total_time_worker)
            averaged_profilers[key]["costs"].append(cost)

    # Calculate baseline times for each chunk size and calculate speed-up
    speedup_cost_data = defaultdict(list)
    for chunk_size in set(cs for _, cs in averaged_profilers.keys()):
        # Find the baseline time for this chunk size
        baseline_times = [
            np.mean(values["times"])
            for (mem, cs), values in averaged_profilers.items()
            if cs == chunk_size
        ]
        baseline_time = min(baseline_times) if baseline_times else float("inf")

        # Calculate speed-up and average cost for this chunk size
        for (memory, cs), values in averaged_profilers.items():
            if cs == chunk_size:
                average_time = np.mean(values["times"])
                average_cost = np.mean(values["costs"])
                speedup = calculate_speedup(baseline_time, average_time)
                speedup_cost_data[chunk_size].append((speedup, average_cost))

    # Setup the plot
    plt.figure(figsize=(15, 10))
    plt.title("Cost vs Average Speed-up for Each Chunk Size Configuration", fontsize=18)

    # Plot each chunk size's speed-up against cost
    for chunk_size, speedup_costs in speedup_cost_data.items():
        speedups, costs = zip(*speedup_costs)
        plt.scatter(speedups, costs, label=f"{chunk_size} MB Chunk")

    plt.xlabel("Average Speed-up", fontsize=16)
    plt.ylabel("Cost", fontsize=16)
    plt.xscale("log")  # Use logarithmic scale for better visualization if needed
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.legend(fontsize=14)
    plt.grid(True)
    plt.tight_layout()

    # Save the plot
    save_path = os.path.join(save_dir, "cost_vs_average_speedup.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Cost vs Speed-up plot saved to: {save_path}")


def plot_memory_speedup_from_collection(job_collection, save_dir):
    execution_times = defaultdict(lambda: defaultdict(list))

    # Process the collection data
    for step_name, job in job_collection:
        for profiler in job.profilers:
            total_time_worker = (
                profiler.worker_end_tstamp - profiler.worker_start_tstamp
            )
            key = (job.memory, job.chunk_size)
            execution_times[key]["times"].append(total_time_worker)

    # Calculate the baseline times for each chunk size
    baseline_times = {}
    for key, times in execution_times.items():
        _, chunk_size = key
        if chunk_size not in baseline_times:
            baseline_times[chunk_size] = min(
                np.mean(times["times"])
                for (mem, cs) in execution_times
                if cs == chunk_size
            )

    # Calculate speed-up for each memory configuration
    speedup_data = defaultdict(list)
    for (memory, chunk_size), data in list(execution_times.items()):
        average_time = np.mean(data["times"])
        baseline_time = baseline_times[chunk_size]
        speedup = calculate_speedup(baseline_time, average_time)
        speedup_data[chunk_size].append((memory, speedup))

    plt.figure(figsize=(15, 10))
    plt.title("Speed-up When Increasing Runtime Memory", fontsize=18)

    for chunk_size, memory_speedups in speedup_data.items():
        memory_speedups.sort(key=lambda x: x[0])
        memories, speedups = zip(*memory_speedups)
        plt.plot(memories, speedups, marker="o", label=f"Chunk Size {chunk_size} MB")

    plt.xlabel("Memory (MB)", fontsize=16)
    plt.ylabel("Speed-up", fontsize=16)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.legend(fontsize=14)
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "memory_speedup.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Memory Speed-up plot saved to: {save_path}")


def plot_cost_vs_time_pareto_simulated(collection, save_dir, dataset_size: int = 1100):
    cost_per_ms_per_mb = 0.0000000167
    averaged_profilers = defaultdict(lambda: defaultdict(list))

    # Process the collection data
    for step_profiler in collection:
        for profiler in step_profiler.profilers:
            total_time = sum(timer.duration for timer in profiler.function_timers)
            cost = (
                total_time * 1000 * cost_per_ms_per_mb * (step_profiler.memory / 1024)
            )
            key = (step_profiler.memory, step_profiler.chunk_size)
            averaged_profilers[key]["times"].append(total_time)
            averaged_profilers[key]["costs"].append(cost)

    costs = []
    times = []
    details_for_dataset = []
    for (memory, chunk_size), values in averaged_profilers.items():
        average_time = sum(values["times"]) / len(values["times"])
        num_chunks = math.ceil(dataset_size / chunk_size)

        total_cost = (
            average_time * num_chunks * 1000 * cost_per_ms_per_mb * (memory / 1024)
        )

        costs.append(total_cost)
        times.append(average_time)
        details_for_dataset.append((chunk_size, memory, num_chunks))

    pareto_costs, pareto_times, pareto_details = find_pareto(
        costs, times, details_for_dataset
    )

    plt.figure(figsize=(15, 10))
    plt.title(
        f"Pareto Analysis: Cost vs Execution Time for Processing {dataset_size} MB"
    )

    plt.scatter(times, costs, color="grey", label="All Points")
    plt.scatter(
        pareto_times,
        pareto_costs,
        color="red",
        edgecolor="black",
        label="Pareto Optimal Points",
    )
    for i, (cost, time, detail) in enumerate(
        zip(pareto_costs, pareto_times, pareto_details)
    ):
        chunk_size, memory, num_workers = detail
        annotation_text = f"{chunk_size} MB\n{memory} MB\n{num_workers} workers"

        offset_index = (i % 3) - 1
        xytext_offset = (offset_index * 60, 30 + offset_index * 20)

        plt.annotate(
            annotation_text,
            xy=(time, cost),
            xytext=xytext_offset,
            textcoords="offset points",
            arrowprops=dict(arrowstyle="->", color="black"),
            ha="center",
            fontsize=8,
        )

    plt.xlabel("Execution Time (seconds)")
    plt.ylabel("Cost")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(save_dir, f"pareto_analysis_for_{dataset_size}MB.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Pareto plot saved to: {save_path}")


def plot_cost_vs_time_pareto_real(job_collection, save_dir, step_name, dataset_size):
    cost_per_ms_per_mb = 0.0000000167
    data_for_plot = defaultdict(list)
    average_data = defaultdict(lambda: defaultdict(list))

    for step, job in job_collection:
        if step == step_name:
            runtime_mem, chunk_size = job.memory, job.chunk_size
            acc_cost = 0
            for worker in job.profilers:
                acc_cost += (
                    (worker.worker_end_tstamp - worker.worker_start_tstamp)
                    * 1000
                    * cost_per_ms_per_mb
                    * (runtime_mem / 1024)
                )
            job_key = (chunk_size, runtime_mem, len(job.profilers))
            average_data[job_key]["times"].append(job.end_time - job.start_time)
            average_data[job_key]["costs"].append(acc_cost)

    for key, values in average_data.items():
        avg_time = np.median(values["times"])
        avg_cost = np.median(values["costs"])
        std_time = np.std(values["times"])
        std_cost = np.std(values["costs"])
        data_for_plot[key[0]].append(
            (avg_time, avg_cost, std_time, std_cost, key[1], key[2])
        )

    plt.figure(figsize=(15, 10))
    colors = plt.cm.rainbow(np.linspace(0, 1, len(data_for_plot)))

    for i, (chunk_size, data) in enumerate(data_for_plot.items()):
        sorted_data = sorted(data, key=lambda x: x[4])
        times, costs, std_times, std_costs, memories, num_workers = zip(*sorted_data)

        plt.errorbar(
            times,
            costs,
            xerr=std_times,
            yerr=std_costs,
            fmt="o",
            color=colors[i],
            alpha=0.5,
            capsize=5,
        )
        plt.plot(times, costs, color=colors[i], alpha=0.5)

    all_data = [(d[0], d[1]) for data in data_for_plot.values() for d in data]
    all_times, all_costs = zip(*all_data)
    pareto_front = is_pareto_efficient(np.vstack((all_times, all_costs)).T)
    pareto_times = np.array(all_times)[pareto_front]
    pareto_costs = np.array(all_costs)[pareto_front]

    plt.scatter(
        pareto_times,
        pareto_costs,
        color="red",
        edgecolor="black",
        label="Pareto Frontier",
        zorder=3,
    )

    texts = []
    for chunk_size, data in data_for_plot.items():
        for time, cost, std_time, std_cost, mem, workers in data:
            annotation_text = f"{mem} MB, {workers} workers"
            texts.append(
                plt.text(
                    time + 0.5,
                    cost,
                    annotation_text,
                    ha="left",
                    va="bottom",
                    fontsize=12,
                )
            )

    adjust_text(texts, arrowprops=dict(arrowstyle="->", color="black", lw=0.5))

    plt.title(
        f"Pareto Analysis: Cost vs Execution Time for {step_name}, Dataset Size {dataset_size} MB without partitioning costs",
        fontsize=18,
    )
    plt.xlabel("Execution Time (seconds)", fontsize=16)
    plt.ylabel("Cost", fontsize=16)

    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    plt.legend(
        [plt.Line2D([0], [0], color=c, lw=4) for c in colors]
        + [
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                label="Pareto Frontier",
                markersize=10,
                markerfacecolor="red",
            )
        ],
        [f"Chunk Size: {cs} MB" for cs in data_for_plot.keys()] + ["Pareto Frontier"],
        fontsize=14,
    )
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(save_dir, f"pareto_analysis_{step_name}.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Pareto plot saved to: {save_path}")


def load_partitioning_times(results_file):
    with open(results_file, "r") as file:
        data = json.load(file)

    partitioning_times = {}
    partitions_data = data.get("8", {})

    for num_partitions, results_list in partitions_data.items():
        total_times = [
            result["total_time"] for result in results_list if "total_time" in result
        ]
        if total_times:
            partitioning_times[num_partitions] = np.mean(total_times)

    return partitioning_times


def plot_cost_vs_time_pareto_real_partition(
    job_collection, save_dir, step_name, dataset_size, results_file
):
    cost_per_ms_per_mb = 0.0000000167
    cost_per_second_partitioning = 9.44444444e-5
    data_for_plot = defaultdict(list)
    average_data = defaultdict(lambda: defaultdict(list))

    partitioning_times = load_partitioning_times(results_file)

    for step, job in job_collection:
        if step == step_name:
            runtime_mem, chunk_size = job.memory, job.chunk_size
            num_workers = len(job.profilers)
            partition_time = partitioning_times.get(str(num_workers), 0)
            acc_cost = 0
            for worker in job.profilers:
                acc_cost += (
                    (worker.worker_end_tstamp - worker.worker_start_tstamp)
                    * 1000
                    * cost_per_ms_per_mb
                    * (runtime_mem / 1024)
                )
            partitioning_cost = partition_time * cost_per_second_partitioning
            total_cost = acc_cost + partitioning_cost

            job_key = (chunk_size, runtime_mem, num_workers)
            total_time = job.end_time - job.start_time + partition_time
            average_data[job_key]["times"].append(total_time)
            average_data[job_key]["costs"].append(total_cost)

    for key, values in average_data.items():
        if values["times"] and values["costs"]:
            avg_time = np.median(values["times"])
            avg_cost = np.median(values["costs"])
            std_time = np.std(values["times"])
            std_cost = np.std(values["costs"])
            data_for_plot[key[0]].append(
                (avg_time, avg_cost, std_time, std_cost, key[1], key[2])
            )

    plt.figure(figsize=(15, 10))
    if data_for_plot: 
        colors = plt.cm.rainbow(np.linspace(0, 1, len(data_for_plot)))
        for i, (chunk_size, data) in enumerate(data_for_plot.items()):
            sorted_data = sorted(data, key=lambda x: x[4])
            times, costs, std_times, std_costs, memories, num_workers = zip(
                *sorted_data
            )

            plt.errorbar(
                times,
                costs,
                xerr=std_times,
                yerr=std_costs,
                fmt="o",
                color=colors[i],
                alpha=0.5,
                capsize=5,
            )
            plt.plot(times, costs, color=colors[i], alpha=0.5)

        all_data = [(d[0], d[1]) for data in data_for_plot.values() for d in data]
        if all_data:  
            all_times, all_costs = zip(*all_data)
            pareto_front = is_pareto_efficient(np.vstack((all_times, all_costs)).T)
            pareto_times = np.array(all_times)[pareto_front]
            pareto_costs = np.array(all_costs)[pareto_front]

            plt.scatter(
                pareto_times,
                pareto_costs,
                color="red",
                edgecolor="black",
                label="Pareto Frontier",
                zorder=3,
            )

        texts = []
        for chunk_size, data in data_for_plot.items():
            for time, cost, std_time, std_cost, mem, workers in data:
                annotation_text = f"{mem} MB, {workers} workers"
                texts.append(
                    plt.text(
                        time + 0.5,
                        cost + (max(costs)*0.02),  
                        annotation_text,
                        ha="left",
                        va="bottom",
                        fontsize=12,
                    )
                )

        adjust_text(texts, arrowprops=dict(arrowstyle="->", color="black", lw=0.5))

        legend_elements = [plt.Line2D([0], [0], color=c, lw=4) for c in colors] + [
            plt.Line2D([0], [0], marker='o', color='w', label='Pareto Frontier', markerfacecolor='red', markersize=10)]
        plt.legend(legend_elements, [f"Chunk Size: {cs} MB" for cs in data_for_plot.keys()] + ["Pareto Frontier"], fontsize=14, loc='best')

        plt.title(
            f"Pareto Analysis: Cost vs Execution Time for {step_name}, Dataset Size {dataset_size} MB including partitioning costs",
            fontsize=18,
        )
        plt.xlabel("Execution Time (seconds)", fontsize=16)
        plt.ylabel("Cost", fontsize=16)
        plt.xticks(fontsize=14)
        plt.yticks(fontsize=14)
        plt.grid(True)
        plt.tight_layout()
    else:
        plt.text(
            0.5,
            0.5,
            "No data available for plotting",
            horizontalalignment="center",
            verticalalignment="center",
            fontsize=20,
            color="red",
        )

    save_path = os.path.join(
        save_dir, f"pareto_analysis_{step_name}_{dataset_size}MB.png"
    )
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Pareto plot saved to: {save_path}")
