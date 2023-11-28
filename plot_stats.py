import pandas as pd
import matplotlib.pyplot as plt
import os


# Function to read the Excel sheet into a DataFrame
def read_sheet(excel_path, sheet_name):
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=1)
    df.set_index(df.columns[0], inplace=True)
    return df


excel_path = "time_stats.xlsx"

plots_dir = "plots"
os.makedirs(plots_dir, exist_ok=True)


def plot_operation_times(mean_df, std_df, operation_name):
    plt.figure(figsize=(14, 8))
    for runtime_memory in mean_df.columns:
        plt.plot(
            mean_df.index, mean_df[runtime_memory], "-o", label=f"{runtime_memory} MB"
        )
        plt.fill_between(
            mean_df.index,
            mean_df[runtime_memory] - std_df[runtime_memory],
            mean_df[runtime_memory] + std_df[runtime_memory],
            alpha=0.2,
        )

    plt.xlabel("Input Size (MB)")
    plt.ylabel(f"{operation_name} (seconds)")
    plt.title(f"{operation_name} vs Input Size for Various Worker Configurations")
    plt.legend(title="Worker Config")
    plt.grid(True)

    plot_filename = f"{operation_name.replace(' ', '_')}.png"
    plt.savefig(os.path.join(plots_dir, plot_filename))
    plt.close()  # Close the figure to free up memory


operations = {
    "download_ms": "Download Time",
    "execute_script": "Execution Time",
    "upload_rebinnedms": "Upload Time",
}

for operation_key, operation_name in operations.items():
    mean_sheet_title = f"{operation_key} Mean"
    std_sheet_title = f"{operation_key} Std Dev"

    mean_df = read_sheet(excel_path, mean_sheet_title)
    std_df = read_sheet(excel_path, std_sheet_title)

    plot_operation_times(mean_df, std_df, operation_name)
