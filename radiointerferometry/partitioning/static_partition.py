from casacore.tables import table
import os
import concurrent.futures
import shutil
import numpy as np
from radiointerferometry.datasource import LithopsDataSource, OutputS3, InputS3
from pathlib import PosixPath

MB = 1024 * 1024


def create_partition(i, start_row, end_row, ms, msout):
    datasource = LithopsDataSource()

    print(f"Creating partition {i} with rows from {start_row} to {end_row - 1}...")
    partition = ms.selectrows(list(range(start_row, end_row)))
    partition_name = PosixPath(f"partition_{i}.ms")
    partition.copy(str(partition_name), deep=True)
    partition.close()

    partition_size = get_dir_size(partition_name)
    print(f"Partition {i} created. Size before zip: {partition_size} bytes")

    zip_filepath = datasource.zip_without_compression(partition_name)
    print(f"Partition {i} zipped at {zip_filepath}")

    # Get the file size before deleting the file
    zip_file_size = os.path.getsize(zip_filepath)
    print(f"Zip file size: {zip_file_size} bytes")

    datasource.upload_file(zip_filepath, msout)

    os.remove(zip_filepath)
    shutil.rmtree(partition_name)

    print(f"Partition {i} uploaded.")

    return zip_filepath, partition_size


def partition_ms(msin, num_partitions, msout):
    print(f"Starting partitioning of {msin} into {num_partitions} partitions...")

    datasource = LithopsDataSource()
    ms_to_part = datasource.download_directory(msin, PosixPath("/tmp"))

    print(f"Downloaded files to: {ms_to_part}")
    full_file_paths = [PosixPath(ms_to_part) / f for f in os.listdir(ms_to_part)]
    print(f"Files ready to be processed: {full_file_paths}")

    mss = []
    for f_path in full_file_paths:
        if not f_path.exists():
            print(f"File not found: {f_path}")
            continue
        print(f"Processing file: {f_path}")
        unzipped_ms = datasource.unzip(f_path)
        print(f"Unzipped contents at: {unzipped_ms}")

        ms_table = table(str(unzipped_ms), ack=False)
        mss.append(ms_table)

    ms = table(mss)

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
            executor.submit(
                create_partition, info[0], info[1], info[2], ms_sorted, msout
            )
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

    return InputS3(bucket=msout.bucket, key=msout.key)


def get_dir_size(start_path="."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size
