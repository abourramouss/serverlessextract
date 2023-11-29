import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib.cm as cm
import numpy as np
import matplotlib.lines as mlines


def read_sheet(excel_path, sheet_name):
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=1)
    df.set_index(df.columns[0], inplace=True)
    return df


# Function to calculate the total costs
def calculate_total_costs(excel_path, cost_per_ms_per_mb, runtime_memories):
    # Read the mean times for download, execution, and upload
    download_times = read_sheet(excel_path, "download_ms Mean")
    execution_times = read_sheet(excel_path, "execute_script Mean")
    upload_times = read_sheet(excel_path, "upload_rebinnedms Mean")

    # Initialize a DataFrame to store the total costs
    total_costs_df = pd.DataFrame(index=execution_times.index)

    # Calculate costs for each runtime memory configuration
    for col in runtime_memories:
        runtime_memory_mb = int(col)  # The runtime memory in MB
        total_costs_df[col] = (
            (download_times[col] + execution_times[col] + upload_times[col])
            * 1000  # Convert seconds to milliseconds
            * cost_per_ms_per_mb  # Cost per millisecond per MB
            * runtime_memory_mb
            / 1024  # Adjust for memory size
        )
    return total_costs_df


# Define the Excel file path and the cost per ms per MB
excel_path = "time_stats.xlsx"  # Update to the correct path
cost_per_ms_per_mb = 0.0000000167

# Ensure the 'plots' directory exists
plots_dir = "plots"
os.makedirs(plots_dir, exist_ok=True)


# Read the execution time data from the Excel file
execution_time_df = read_sheet(excel_path, "execute_script Mean")

# Calculate the costs for each runtime memory
runtime_memories = execution_time_df.columns.tolist()  # Convert columns to a list
costs_df = calculate_total_costs(excel_path, cost_per_ms_per_mb, runtime_memories)


def plot_all_to_all_comparison(execution_time_df, costs_df, max_chunk_size):
    plt.figure(figsize=(14, 8))
    # Generate a color map with a unique color for each chunk size.
    colors = cm.rainbow(np.linspace(0, 1, len(execution_time_df.index)))

    # To store legend handles
    legend_handles = []

    for i, chunk_size in enumerate(execution_time_df.index):
        num_chunks = max_chunk_size / chunk_size  # Number of chunks
        adjusted_costs = costs_df.loc[chunk_size] * num_chunks  # Adjust costs
        execution_times = (
            execution_time_df.loc[chunk_size] * num_chunks
        )  # Adjust execution times
        color = colors[i]

        # Plot each point with its corresponding runtime memory size and chunk size
        for runtime_memory in execution_time_df.columns:
            exec_time = execution_times[runtime_memory]
            cost = adjusted_costs[runtime_memory]
            plt.scatter(exec_time, cost, color=color)
            plt.text(
                exec_time,
                cost,
                f"{runtime_memory} MB",
                fontsize=9,
                ha="center",
                va="bottom",
            )

        # Sort execution_times for drawing a line across points that have the same chunk size
        sorted_runtime_memories = sorted(
            execution_time_df.columns, key=lambda x: execution_times[x]
        )
        sorted_exec_times = [execution_times[rm] for rm in sorted_runtime_memories]
        sorted_costs = [adjusted_costs[rm] for rm in sorted_runtime_memories]

        # Draw a line across points that have the same chunk size
        line = plt.plot(
            sorted_exec_times, sorted_costs, color=color, linestyle="-", linewidth=2
        )[0]

        # Create a legend entry for this chunk size
        legend_handles.append(
            mlines.Line2D(
                [],
                [],
                color=color,
                label=f"{chunk_size} MB Chunk",
                linestyle="-",
                linewidth=2,
            )
        )

    plt.xlabel("Execution Time (seconds)")
    plt.ylabel("Cost")
    plt.title(
        f"Cost vs Execution Time for Various Chunk Sizes up to {max_chunk_size} MB"
    )
    plt.legend(handles=legend_handles, title="Chunk Size")
    plt.grid(True)

    # Ensure the output directory exists
    os.makedirs(plots_dir, exist_ok=True)
    plot_filename = os.path.join(
        plots_dir, f"all_to_all_cost_vs_time_comparison_up_to_{max_chunk_size}_MB.png"
    )
    plt.savefig(plot_filename)
    plt.close()


# Plotting function to create cost plots with execution time on x-axis and cost on y-axis
def plot_costs(execution_time_df, costs_df, chunk_size):
    plt.figure(figsize=(14, 8))

    # Get the execution times and costs for the given chunk size
    execution_times = execution_time_df.loc[chunk_size]
    costs = costs_df.loc[chunk_size]

    # Plot the costs against the execution times
    for runtime_memory in execution_time_df.columns:
        exec_time = execution_times[runtime_memory]
        cost = costs[runtime_memory]

        # Check if both exec_time and cost are finite numbers
        if np.isfinite(exec_time) and np.isfinite(cost):
            plt.scatter(exec_time, cost, label=f"{runtime_memory} MB")
            plt.text(
                exec_time,
                cost,
                f"{runtime_memory} MB",
                fontsize=9,
                ha="right",
                va="bottom",
            )

    plt.xlabel("Execution Time (seconds)")
    plt.ylabel("Cost")
    plt.title(f"Cost vs Execution Time for Chunk Size {chunk_size} MB")
    plt.legend(title="Runtime Memory")
    plt.grid(True)

    # Save the plot in the 'plots' directory
    plot_filename = f"cost_vs_execution_time_chunk_size_{chunk_size}.png"
    plt.savefig(os.path.join(plots_dir, plot_filename))
    plt.close()  # Close the figure after saving to free up memory


# Plot the costs for a specific chunk size (you can loop over different chunk sizes if needed)
chunk_sizes = [129, 263, 526, 1128, 2633, 3950]
for chunk_size in chunk_sizes:
    plot_costs(execution_time_df, costs_df, chunk_size)


plot_all_to_all_comparison(execution_time_df, costs_df, 3950)
