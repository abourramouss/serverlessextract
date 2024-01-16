import os
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
import math
from profiling import JobCollection


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
            text_x, text_y, f"{data['avg_duration']:.2f}s", va="center", ha="center"
        )

    ax.set_yticks([0.5 + i for i in range(len(timer_data))])
    ax.set_yticklabels([label for label in timer_data])
    ax.set_ylim(-1, len(timer_data))
    ax.set_xlabel("Average Time (s) since Start")
    ax.set_ylabel("Function Timers")
    ax.set_title("Gantt Chart of Averaged Function Timers")

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
        "Cost vs Execution Time for Various Chunk Sizes and Memory Configurations"
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

        # Assign the color for the current chunk size
        color = color_map[chunk_size]

        plt.scatter(times, costs, color=color, label=f"{chunk_size} MB Chunk")
        plt.plot(times, costs, color=color)  # Connect points of the same chunk size
        # Highlight the optimal point
        plt.scatter(
            [optimal_time], [optimal_cost], color=color, edgecolor="black", marker="D"
        )
        plt.text(
            optimal_time,
            optimal_cost,
            f"{optimal_memory} MB (Optimal)",
            fontsize=9,
            ha="right",
            va="bottom",
        )

    plt.xlabel("Execution Time (seconds)")
    plt.ylabel("Cost")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "averaged_cost_vs_time_job_collection.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Plot saved to: {save_path}")


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

        # Calculate total cost based on average duration and number of chunks
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

            data_for_plot[chunk_size].append(
                (
                    job.end_time - job.start_time,
                    acc_cost,
                    runtime_mem,
                    chunk_size,
                    len(job.profilers),
                )
            )

    plt.figure(figsize=(15, 10))
    colors = plt.cm.rainbow(np.linspace(0, 1, len(data_for_plot)))

    for i, (chunk_size, data) in enumerate(data_for_plot.items()):
        times, costs, memories, num_workers = zip(
            *[(d[0], d[1], d[2], d[4]) for d in data]
        )
        plt.scatter(times, costs, color=colors[i], alpha=0.5)
        plt.plot(times, costs, color=colors[i], alpha=0.5)

    all_times, all_costs = zip(
        *[(d[0], d[1]) for data in data_for_plot.values() for d in data]
    )
    pareto_front = is_pareto_efficient(np.vstack((all_times, all_costs)).T)
    plt.scatter(
        np.array(all_times)[pareto_front],
        np.array(all_costs)[pareto_front],
        color="red",
    )

    annotations = []
    for chunk_size, data in data_for_plot.items():
        for time, cost, mem, workers in [(d[0], d[1], d[2], d[4]) for d in data]:
            annotation_text = f"{mem} MB, {cost:.4f}, {workers} workers, {time:.2f}s"
            annotations.append((time, cost, annotation_text))

    def check_overlap(annotation, other_annotations):
        for other in other_annotations:
            if (abs(annotation[0] - other[0]) < 0.05) and (
                abs(annotation[1] - other[1]) < 0.05
            ):
                return True
        return False

    for time, cost, annotation_text in annotations:
        xytext = (20, 20)
        while check_overlap((time + xytext[0], cost + xytext[1]), annotations):
            xytext = (xytext[0] + 10, xytext[1] + 10)

        plt.annotate(
            annotation_text,
            xy=(time, cost),
            xytext=xytext,
            textcoords="offset points",
            ha="right",
            va="bottom",
            arrowprops=dict(
                arrowstyle="->", connectionstyle="arc3,rad=.2", color="black"
            ),
            fontsize=6,
        )

    plt.title(
        f"Pareto Analysis: Cost vs Execution Time for {step_name}, Dataset Size {dataset_size} MB"
    )
    plt.xlabel("Execution Time (seconds)")
    plt.ylabel("Cost")
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
    )

    plt.grid(True)
    plt.tight_layout()
    save_path = os.path.join(save_dir, f"pareto_analysis_{step_name}.png")
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(save_path)
    plt.close()
    print(f"Pareto plot saved to: {save_path}")
