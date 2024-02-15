import pandas as pd
import matplotlib.pyplot as plt
import json

# Load the data from 'results.json'
with open("results.json", "r") as file:
    data = json.load(file)

# Transforming loaded data into a DataFrame-friendly format
records = []
for num_cpus, partitions in data.items():
    for num_partitions, results in partitions.items():
        for result in results:
            records.append(
                {
                    "num_cpus": num_cpus,  # This will be converted to int
                    "num_partitions": num_partitions,  # This will be converted to int
                    "output_size": result.get("output_size", 0),  # Capture output size
                }
            )

# Creating DataFrame from records
df = pd.DataFrame(records)

# Convert 'num_cpus' and 'num_partitions' to integers
df["num_cpus"] = df["num_cpus"].astype(int)
df["num_partitions"] = df["num_partitions"].astype(int)

# Since output sizes are consistent across CPU configurations, select unique partition counts
plot_data = df.drop_duplicates(subset=["num_partitions"], keep="first")[
    ["num_partitions", "output_size"]
].sort_values(by="num_partitions")

# Fixed input size
input_size = 1091

# Calculate the difference between output size and input size
plot_data["output_input_difference"] = plot_data["output_size"] - input_size

# Plotting the difference between output size and input size
fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(
    plot_data["num_partitions"],
    plot_data["output_input_difference"],
    label="Output - Input Size Difference",
    marker="o",
    linestyle="-",
)

ax.set_xlabel("Number of Partitions")
ax.set_ylabel("Output - Input Size Difference (MB)")
ax.set_title("Output - Input Size Difference vs. Number of Partitions")
ax.legend()

# Save the plot to a file
plt.savefig("output_input_difference_analysis.png")
