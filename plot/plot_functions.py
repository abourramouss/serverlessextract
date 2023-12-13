import matplotlib.pyplot as plt
import os


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
                aggregated_metrics[cid]["net_read_mb"] += getattr(
                    metric, "net_read_mb", 0
                )
                aggregated_metrics[cid]["net_write_mb"] += getattr(
                    metric, "net_write_mb", 0
                )

    min_timestamp = min(min(ts) for ts in timestamps.values())
    relative_timestamps = {
        cid: [t - min_timestamp for t in ts] for cid, ts in timestamps.items()
    }

    sorted_cids = sorted(relative_timestamps.keys())
    relative_times = [relative_timestamps[cid][0] for cid in sorted_cids]
    cpu_usages = [aggregated_metrics[cid]["cpu_usage"] for cid in sorted_cids]
    memory_usages = [aggregated_metrics[cid]["memory_usage"] for cid in sorted_cids]
    disk_read_rates = [0]  # Initialize with zero for the first rate
    disk_write_rates = [0]  # Initialize with zero for the first rate
    net_read_rates = [0]  # Initialize with zero for the first rate
    net_write_rates = [0]  # Initialize with zero for the first rate

    for i in range(1, len(sorted_cids)):
        cid = sorted_cids[i]
        prev_cid = sorted_cids[i - 1]
        time_diff = relative_timestamps[cid][0] - relative_timestamps[prev_cid][0]

        disk_read_rate = (
            aggregated_metrics[cid]["disk_read_mb"]
            - aggregated_metrics[prev_cid]["disk_read_mb"]
        ) / max(time_diff, 1)
        disk_write_rate = (
            aggregated_metrics[cid]["disk_write_mb"]
            - aggregated_metrics[prev_cid]["disk_write_mb"]
        ) / max(time_diff, 1)
        net_read_rate = (
            aggregated_metrics[cid]["net_read_mb"]
            - aggregated_metrics[prev_cid]["net_read_mb"]
        ) / max(time_diff, 1)
        net_write_rate = (
            aggregated_metrics[cid]["net_write_mb"]
            - aggregated_metrics[prev_cid]["net_write_mb"]
        ) / max(time_diff, 1)

        disk_read_rates.append(disk_read_rate)
        disk_write_rates.append(disk_write_rate)
        net_read_rates.append(net_read_rate)
        net_write_rates.append(net_write_rate)

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
    plt.subplot(3, 2, 3)
    plt.plot(relative_times, disk_read_rates, marker="o")
    plt.title("Disk Read Rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Disk Read Rate (MB/s)")
    plt.subplot(3, 2, 4)
    plt.plot(relative_times, disk_write_rates, marker="o")
    plt.title("Disk Write Rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Disk Write Rate (MB/s)")
    plt.subplot(3, 2, 5)
    plt.plot(relative_times, net_read_rates, marker="o")
    plt.title("Network Read Rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Network Read Rate (MB/s)")
    plt.subplot(3, 2, 6)
    plt.plot(relative_times, net_write_rates, marker="o")
    plt.title("Network Write Rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Network Write Rate (MB/s)")
    plt.tight_layout()
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(os.path.join(save_dir, filename))
    plt.close()
