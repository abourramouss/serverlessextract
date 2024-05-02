import lithops
import hashlib
from casacore.tables import table
import os
import zipfile
import concurrent.futures
import io
import shutil
import numpy as np
from pathlib import PosixPath
from s3path import S3Path
import sys

sys.path.append("/home/users/ayman/ayman/extract-project/serverlessextract")
from datasource import LithopsDataSource


MB = 1024 * 1024
cache = {}


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


def generate_ms_hash(ms_paths, num_partitions):
    hash_md5 = hashlib.md5()
    for ms_path in ms_paths:
        s3_uri = str(ms_path)
        hash_md5.update(s3_uri.encode())
    hash_md5.update(str(num_partitions).encode())
    return hash_md5.hexdigest()


def partition_ms_lithops(ms_path, num_partitions, total_partitions):
    data_source = LithopsDataSource()
    local_ms_path = data_source.download_directory(S3Path(ms_path))
    worker_partitions = num_partitions // total_partitions
    partition_sizes = partition_ms(local_ms_path, worker_partitions)
    partition_file_paths = []
    for i, partition_size in enumerate(partition_sizes):
        partition_file = f"partition_{i}.zip"
        partition_file_path = S3Path(
            f"ayman-extract/partitions/{ms_path.key}/{worker_partitions}/{partition_file}"
        )
        data_source.upload_file(PosixPath(partition_file), partition_file_path)
        partition_file_paths.append(str(partition_file_path))
    return partition_file_paths


def partition_measurement_sets(measurement_sets_path, num_partitions):
    measurement_sets = lithops.Storage().list_keys(
        bucket=measurement_sets_path.bucket,
        prefix=f"{measurement_sets_path.key}/",
    )
    ms_hash = generate_ms_hash(measurement_sets, num_partitions)
    cache_key = f"{ms_hash}_{num_partitions}"
    cached_partitions = cache.get(cache_key)
    if cached_partitions:
        return cached_partitions
    fexec = lithops.FunctionExecutor()
    futures = fexec.map(
        partition_ms_lithops,
        measurement_sets,
        extra_args=[num_partitions, len(measurement_sets)],
    )
    results = fexec.get_result(futures)
    flattened_results = [item for sublist in results for item in sublist]
    cache[cache_key] = flattened_results
    return flattened_results


measurement_sets_path = S3Path("/ayman-extract/partitions/partitions_7900_1")

num_partitions = 4
partitioned_sets = partition_measurement_sets(measurement_sets_path, num_partitions)
