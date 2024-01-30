from casacore.tables import table, tablecopy
import os
import zipfile
import concurrent.futures
import io
import shutil
import time
import json
from lithops import Storage


def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, "..")),
            )


def create_partition(i, start_row, end_row, ms):
    partition = ms.selectrows(list(range(start_row, end_row)))
    partition_name = f"partition_{i}.ms"
    partition.copy(partition_name, deep=True)
    partition.close()

    partition_rows = end_row - start_row

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zipf:
        zipdir(partition_name, zipf)
    shutil.rmtree(partition_name)

    partition_file = f"partition_{i}.zip"
    with open(partition_file, "wb") as file:
        file.write(zip_buffer.getvalue())

    return partition_file, partition_rows


def partition_ms(ms_path, num_partitions):
    ms = table(ms_path, ack=False)
    total_rows = ms.nrows()
    rows_per_partition = total_rows // num_partitions

    print(f"Initial dataset size: {get_dir_size(ms_path) / 1024 / 1024} MB")
    print(f"Initial dataset rows: {total_rows}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                create_partition,
                i,
                i * rows_per_partition,
                (i * rows_per_partition) + rows_per_partition
                if i < num_partitions - 1
                else total_rows,
                ms,
            )
            for i in range(num_partitions)
        ]
        results = [f.result() for f in futures]

    ms.close()

    total_partition_size = sum([os.path.getsize(r[0]) for r in results])
    total_partition_rows = sum([r[1] for r in results])

    print(f"Total partitions created: {num_partitions}")
    print(f"Total partition sizes: {total_partition_size / 1024 / 1024} MB")
    print(f"Total partition rows: {total_partition_rows}")

    return results


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
    num_executions,
    results_file,
    bucket,
    s3_prefix,
):
    results = []

    for num_partitions in range(min_partitions, max_partitions + 1):
        execution_times = []
        for _ in range(num_executions):
            start_time = time.time()
            partition_results = partition_ms(ms_path, num_partitions)
            end_time = time.time()
            execution_times.append(end_time - start_time)

            storage = Storage()
            for partition_file, _ in partition_results:
                s3_key = os.path.join(s3_prefix, os.path.basename(partition_file))
                try:
                    print(f"Uploading {partition_file} to {bucket}/{s3_key}...")
                    storage.upload_file(partition_file, bucket, key=s3_key)
                    print(f"Upload finished for {partition_file}")
                except Exception as e:
                    print(f"An exception occurred: {e}")

        avg_execution_time = sum(execution_times) / num_executions
        results.append(
            {"num_partitions": num_partitions, "avg_execution_time": avg_execution_time}
        )
        print(
            f"Avg execution time for {num_partitions} partitions: {avg_execution_time} seconds"
        )

    with open(results_file, "w") as file:
        json.dump(results, file)

    storage = Storage()
    try:
        print(f"Uploading {results_file} to {bucket}/{s3_key}...")
        storage.upload_file(results_file, bucket, key=s3_key)
        print(f"Upload finished for {results_file}")
    except Exception as e:
        print(f"An exception occurred: {e}")


measure_and_save_results(
    "/home/ayman/Desktop/partition_1.ms",
    2,
    5,
    3,
    "results.json",
    "ayman-extract",
    "partitions/partitions_1100MB_2-5zip",
)
