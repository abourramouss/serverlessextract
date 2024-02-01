import pandas as pd
import matplotlib.pyplot as plt
import json

with open("results.json", "r") as file:
    data = json.load(file)

df = pd.DataFrame(data)

fig, ax = plt.subplots()

cpu_counts = df["num_cpus"].unique()

# For each CPU count, filter the data and plot
for cpu in cpu_counts:
    subset = df[df["num_cpus"] == cpu]
    subset = subset.sort_values("num_partitions")
    ax.plot(subset["num_partitions"], subset["total_time"], label=f"CPUs: {cpu}")

ax.set_xlabel("Number of Partitions")
ax.set_ylabel("Total Time Taken (s)")
ax.set_title("Total Time vs. Number of Partitions for Different CPU Counts")
ax.legend()
plt.savefig("plot.png", dpi=300)
