from casacore.tables import table
import os
import numpy as np
import shutil
import zipfile
import time
import json
from upload import upload_directory_to_s3
import matplotlib.pyplot as plt


def remove(path):
    """Remove a file or directory."""
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # Remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # Remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))


def zip_directory_without_compression(source_dir_path, output_zip_path, partition_name):
    """Create a zip file without compression from a directory."""
    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_STORED) as zipf:
        for root, dirs, files in os.walk(source_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.join(
                    partition_name, os.path.relpath(file_path, start=source_dir_path)
                )
                zipf.write(file_path, arcname)


class Partitioner:
    def __init__(self, *input_files):
        self.input_files = input_files

    def partition_chunks(self, num_chunks):
        """Partition input files into specified number of chunks."""
        partition_start_time = time.time()
        os.makedirs("partitions", exist_ok=True)
        partition_counter = 1

        for input_file in self.input_files:
            t = table(input_file, readonly=False)
            t = t.sort("TIME")
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
                    partition = t.selectrows(np.arange(start_index, i))
                    partition_name = f"partitions/partition_{partition_counter}.ms"
                    partition.copy(partition_name, deep=True)
                    partition.close()
                    start_time = current_time
                    end_time = start_time + chunk_duration
                    start_index = i
                    partition_counter += 1
                if i % 100000 == 0:
                    print(f"Processed {i} rows")
            if start_index < num_rows:
                partition = t.selectrows(np.arange(start_index, num_rows))
                partition_name = f"partitions/partition_{partition_counter}.ms"
                partition.copy(partition_name, deep=True)
                partition.close()

            t.close()

        partition_end_time = time.time()
        partition_time = partition_end_time - partition_start_time
        return partition_counter - 1, partition_time


def save_to_json(data, filename):
    """Save data to a JSON file."""
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)


def load_from_json(filename):
    """Load data from a JSON file."""
    with open(filename, "r") as json_file:
        data = json.load(json_file)
    return data


if __name__ == "__main__":
    partitions = [60]

    results = {}
    for pr in partitions:
        total_process_start_time = time.time()

        p = Partitioner("/home/users/ayman/ayman/extract-project/SB205.MS")
        total_partitions, partition_time = p.partition_chunks(pr)

        dir_partitions = os.listdir("partitions")
        zipped_partitions_dir = "zipped_partitions"
        os.makedirs(zipped_partitions_dir, exist_ok=True)

        start_zip_time = time.time()
        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}"
            zip_file = f"{zipped_partitions_dir}/{partition}.zip"
            if os.path.isdir(partition_dir):
                zip_directory_without_compression(partition_dir, zip_file, partition)
                remove(partition_dir)
        zip_duration = time.time() - start_zip_time

        start_upload_time = time.time()
        upload_directory_to_s3(
            zipped_partitions_dir,
            "ayman-extract",
            f"partitions/partitions_7900_{pr}zip",
        )
        upload_duration = time.time() - start_upload_time

        for zip_file in os.listdir(zipped_partitions_dir):
            remove(os.path.join(zipped_partitions_dir, zip_file))
        shutil.rmtree(zipped_partitions_dir)

        total_process_end_time = time.time()
        total_process_time = total_process_end_time - total_process_start_time

        results[str(pr)] = {
            "total_partitions": total_partitions,
            "partition_time": partition_time,
            "zip_time": zip_duration,
            "upload_time": upload_duration,
            "total_process_time": total_process_time,
        }

    save_to_json(results, "partitioning_results.json")
    print("Results saved in 'partitioning_results.json'")
