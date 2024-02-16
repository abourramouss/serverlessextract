from casacore.tables import table
import os
import zipfile
import concurrent.futures
import io
import shutil
import time
import json
from lithops import Storage
import numpy as np
from datetime import datetime

MB = 1024 * 1024


def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, "..")),
            )


def unzip_file(zip_filepath, dest_path):
    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(dest_path)


def create_partition(i, start_row, end_row, ms):
    print(f"Creating partition {i} with rows from {start_row} to {end_row - 1}...")
    partition = ms.selectrows(list(range(start_row, end_row)))
    partition_name = f"partition_{i}.ms"
    partition.copy(partition_name, deep=True)
    partition.close()

    partition_size = get_dir_size(partition_name)
    print(f"Partition {i} created. Size before zip: {partition_size} bytes")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_STORED) as zipf:
        zipdir(partition_name, zipf)
    shutil.rmtree(partition_name)

    partition_file = f"partition_{i}.zip"
    with open(partition_file, "wb") as file:
        file.write(zip_buffer.getvalue())

    print(
        f"Partition {i} zipped. Zip file size: {os.path.getsize(partition_file)} bytes"
    )

    return partition_file, partition_size


def partition_ms(ms_path, num_partitions):
    print(f"Starting partitioning of {ms_path} into {num_partitions} partitions...")
    ms = table(ms_path, ack=False)
    ms_sorted = ms.sort("TIME")
    total_rows = ms_sorted.nrows()
    print(f"Total rows in the measurement set: {total_rows}")
    times = np.array(ms_sorted.getcol("TIME"))
    total_duration = times[-1] - times[0]
    print(f"Total duration in the measurement set: {total_duration}")

    chunk_duration = total_duration / num_partitions
    partitions_info = []
    start_time = times[0]
    partition_count = 0

    for i in range(num_partitions):
        if i < num_partitions - 1:
            end_time = start_time + chunk_duration
            end_index = np.searchsorted(times, end_time, side="left")
        else:
            end_index = total_rows
        start_index = np.searchsorted(times, start_time, side="left")
        partitions_info.append((partition_count, start_index, end_index))
        start_time = end_time
        partition_count += 1

    partition_sizes = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(create_partition, info[0], info[1], info[2], ms_sorted)
            for info in partitions_info
        ]
        for future in concurrent.futures.as_completed(futures):
            partition_file, partition_size = future.result()
            partition_sizes.append(partition_size)
            print(
                f"Partition file {partition_file} with size {partition_size / MB:.2f} MB created and ready for upload."
            )

    total_partition_size = sum(partition_sizes)
    print(f"Total size of all partitions: {total_partition_size / MB:.2f} MB")

    ms_sorted.close()
    print(f"Partitioning completed. {len(partition_sizes)} partitions created.")

    return partition_sizes


def get_dir_size(start_path="."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def measure_and_save_results(
    ms_path,
    min_partitions,
    max_partitions,
    results_file,
    bucket,
    original_file_size,
    extra_folder,
    num_cpus,
):
    print(f"Starting measurement and saving results...")
    storage = Storage()
    s3_key = "partitions/partitions_7zip/partition_1.ms.zip"
    print(f"Downloading {s3_key}...")
    download_start_time = time.time()
    storage.download_file(bucket, s3_key, "/home/ubuntu/partition_1.ms.zip")
    download_end_time = time.time()
    download_time = download_end_time - download_start_time
    print(f"Download completed in {download_time} seconds.")

    unzip_file("/home/ubuntu/partition_1.ms.zip", "/home/ubuntu/")
    input_size = round(get_dir_size(ms_path) / MB)

    existing_results = {}
    if os.path.exists(results_file):
        with open(results_file, "r") as file:
            try:
                existing_results = json.load(file)
            except json.JSONDecodeError:
                print(
                    "Existing results file is empty or contains invalid JSON. Initializing to empty dictionary."
                )

    for num_partitions in range(min_partitions, max_partitions + 1):
        print(f"Processing {num_partitions} partitions...")
        start_time = time.time()
        partition_sizes = partition_ms(
            ms_path, num_partitions
        )  # Retrieves sizes of created partitions
        end_time = time.time()

        execution_time = end_time - start_time
        output_size = round(
            sum(partition_sizes) / MB
        )  # Sum the sizes of partitions directly

        upload_time = time.time() - start_time - execution_time
        total_time = download_time + execution_time + upload_time
        result = {
            "execution_timestamp": datetime.now().isoformat(),
            "execution_time": execution_time,
            "upload_time": upload_time,
            "input_size": input_size,
            "output_size": output_size,
            "total_time": total_time,
            "download_time": download_time,
        }

        cpus_key = str(num_cpus)
        partitions_key = str(num_partitions)
        if cpus_key not in existing_results:
            existing_results[cpus_key] = {}
        if partitions_key not in existing_results[cpus_key]:
            existing_results[cpus_key][partitions_key] = []
        existing_results[cpus_key][partitions_key].append(
            result
        )  # Append the new result to the existing results

    with open(results_file, "w") as file:
        json.dump(existing_results, file, indent=4)

    s3_key = os.path.join(extra_folder, "results", results_file)
    storage.upload_file(results_file, bucket, s3_key)
    print(f"Results uploaded to {bucket}/{s3_key}")


measure_and_save_results(
    f"/home/ubuntu/original1.ms",
    2,
    10,
    "results.json",
    "ayman-extract",
    1090,
    "partitions",
    2,
)
