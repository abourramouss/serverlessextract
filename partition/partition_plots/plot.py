import pandas as pd
import matplotlib.pyplot as plt
import json

# Load the data from 'results.json', partitioning results are saved there. From the static_partition.py
with open("results.json", "r") as file:
    data = json.load(file)

records = []
for num_cpus, partitions in data.items():
    for num_partitions, results in partitions.items():
        for result in results:
            records.append(
                {
                    "num_cpus": num_cpus,
                    "num_partitions": num_partitions,
                    "total_time": result["total_time"],
                    "upload_time": result.get("upload_time", 0),
                    "execution_time": result.get("execution_time", 0),
                }
            )

df = pd.DataFrame(records)

df["num_cpus"] = df["num_cpus"].astype(int)
df["num_partitions"] = df["num_partitions"].astype(int)

stats = (
    df.groupby(["num_cpus", "num_partitions"])
    .agg(
        mean_total_time=("total_time", "mean"),
        std_total_time=("total_time", "std"),
        mean_upload_time=("upload_time", "mean"),
        std_upload_time=("upload_time", "std"),
        mean_execution_time=("execution_time", "mean"),
        std_execution_time=("execution_time", "std"),
    )
    .reset_index()
)

fig, ax = plt.subplots(figsize=(10, 7))
for cpu in stats["num_cpus"].unique():
    cpu_stats = stats[stats["num_cpus"] == cpu]
    ax.errorbar(
        cpu_stats["num_partitions"],
        cpu_stats["mean_total_time"],
        yerr=cpu_stats["std_total_time"],
        label=f"Total Time - CPUs: {cpu}",
        fmt="-o",
    )
    ax.errorbar(
        cpu_stats["num_partitions"],
        cpu_stats["mean_upload_time"],
        yerr=cpu_stats["std_upload_time"],
        label=f"Upload Time - CPUs: {cpu}",
        fmt="-x",
    )
    ax.errorbar(
        cpu_stats["num_partitions"],
        cpu_stats["mean_execution_time"],
        yerr=cpu_stats["std_execution_time"],
        label=f"Execution Time - CPUs: {cpu}",
        fmt="-^",
    )

ax.set_xlabel("Number of Partitions")
ax.set_ylabel("Time (s)")
ax.set_title("Performance Analysis by CPU Count and Number of Partitions")
ax.legend()

plt.savefig("performance_analysis_corrected.png")
