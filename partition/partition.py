from casacore.tables import table
import os
import numpy as np
import shutil
import zipfile
import time
from upload import upload_directory_to_s3
import matplotlib.pyplot as plt


def remove(path):
    """param <path> could either be relative or absolute."""
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))


def zip_directory_without_compression(source_dir_path, output_zip_path, partition_name):
    # Create a zip file with the specified name
    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_STORED) as zipf:
        # Recursively add all files in the directory to the zip file
        for root, dirs, files in os.walk(source_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Modify the archive name to include 'partition_x' in the path
                arcname = os.path.join(
                    partition_name, os.path.relpath(file_path, start=source_dir_path)
                )
                zipf.write(file_path, arcname=arcname)


# Class that takes a measurement set and partitions it into chunks, then uploads them to s3
class Partitioner:
    def __init__(self, *input_files):
        self.input_files = input_files

    def partition_chunks(self, num_chunks):
        overall_start_time = time.time()
        times_per_partition = []
        os.makedirs("partitions", exist_ok=True)
        partition_counter = 1

        for input_file in self.input_files:
            t = table(input_file, readonly=False)
            t = t.sort("TIME")  # Sort the table
            num_rows = len(t)
            original_times = np.array(t.getcol("TIME"))
            total_duration = original_times[-1] - original_times[0]
            chunk_duration = total_duration / num_chunks

            start_time = original_times[0]
            end_time = start_time + chunk_duration
            start_index = 0

            for i in range(num_rows):
                current_time = original_times[i]
                if current_time >= end_time:
                    partition_start_time = (
                        time.time()
                    )  # Start timing for this partition

                    partition = t.selectrows(np.arange(start_index, i))
                    partition_name = f"partitions/partition_{partition_counter}.ms"
                    partition.copy(partition_name, deep=True)
                    partition.close()

                    partition_end_time = time.time()  # End timing for this partition
                    times_per_partition.append(
                        partition_end_time - partition_start_time
                    )

                    start_time = current_time
                    end_time = start_time + chunk_duration
                    start_index = i
                    partition_counter += 1

                if i % 100000 == 0:
                    print(f"Processed {i} rows")

            if start_index < num_rows:
                # Handle the last partition
                partition_start_time = time.time()
                partition = t.selectrows(np.arange(start_index, num_rows))
                partition_name = f"partitions/partition_{partition_counter}.ms"
                partition.copy(partition_name, deep=True)
                partition.close()
                partition_end_time = time.time()
                times_per_partition.append(partition_end_time - partition_start_time)

            t.close()

        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time
        average_time = sum(times_per_partition) / len(times_per_partition)
        return partition_counter - 1, total_time, average_time


if __name__ == "__main__":
    partitions = [61, 30, 15, 7, 3, 2]

    total_times = []
    average_times = []
    zip_times = []
    upload_times = []

    for pr in partitions:
        p = Partitioner("/home/ayman/Downloads/SB205.MS")
        total_partitions, total_time, average_time = p.partition_chunks(pr)
        total_times.append(total_time)
        average_times.append(average_time)

        dir_partitions = os.listdir("partitions")
        start_zip_time = time.time()
        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}"
            if os.path.isdir(partition_dir):
                zip_directory_without_compression(
                    partition_dir, f"{partition_dir}.zip", partition
                )
        zip_duration = time.time() - start_zip_time
        zip_times.append(zip_duration)

        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}"
            remove(partition_dir)

        start_upload_time = time.time()
        upload_directory_to_s3(
            "partitions",
            "ayman-extract",
            f"partitions/partitions_{pr}zip",
        )
        upload_duration = time.time() - start_upload_time
        upload_times.append(upload_duration)

        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}.zip"
            remove(partition_dir)

    # Plotting and saving the plot
    os.makedirs("rebinning/partitioning", exist_ok=True)
    plt.figure(figsize=(18, 6))

    plt.subplot(1, 3, 1)
    plt.plot(partitions, total_times, marker="o")
    plt.xlabel("Number of Partitions")
    plt.ylabel("Total Time (seconds)")
    plt.title("Total Time for Partitioning vs Number of Partitions")
    plt.gca().invert_xaxis()

    plt.subplot(1, 3, 2)
    plt.plot(partitions, zip_times, marker="o", color="green")
    plt.xlabel("Number of Partitions")
    plt.ylabel("Total Zip Time (seconds)")
    plt.title("Total Zip Time vs Number of Partitions")
    plt.gca().invert_xaxis()

    plt.subplot(1, 3, 3)
    plt.plot(partitions, upload_times, marker="o", color="blue")
    plt.xlabel("Number of Partitions")
    plt.ylabel("Upload Time (seconds)")
    plt.title("Upload Time vs Number of Partitions")
    plt.gca().invert_xaxis()

    plt.tight_layout()
    plot_filename = "rebinning/partitioning/partitioning_performance_plot.png"
    plt.savefig(plot_filename)
    plt.show()
    print(f"Plot saved as {plot_filename}")
