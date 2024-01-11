from casacore.tables import table
import os
import numpy as np
import shutil
import zipfile
from upload import upload_directory_to_s3


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


class Partitioner:
    def __init__(self, *input_files):
        self.input_files = input_files

    def partition_into_n(self, n):
        os.makedirs("partitions", exist_ok=True)
        for input_file in self.input_files:
            t = table(input_file, readonly=False)
            num_rows = len(t)
            rows_per_partition = num_rows // n

            start_index = 0
            for i in range(1, n + 1):
                end_index = min(start_index + rows_per_partition, num_rows)
                if i == n:  # For the last partition, include all remaining rows
                    end_index = num_rows

                partition_name = f"partitions/partition_{i}.ms"
                t.selectrows(np.arange(start_index, end_index)).copy(
                    partition_name, deep=True
                )
                print(f"Partition {i} created with rows {start_index} to {end_index}")
                start_index = end_index

            t.close()

        return n


if __name__ == "__main__":
    num_partitions = 1
    p = Partitioner("/home/ayman/Work/partition_1.ms")
    p.partition_into_n(num_partitions)
    print(f"Total partitions created: {num_partitions}")

    dir_partitions = os.listdir("partitions")
    # Zip each partition directory
    for partition in dir_partitions:
        partition_dir = f"partitions/{partition}"
        if os.path.isdir(partition_dir):
            zip_directory_without_compression(
                partition_dir, f"{partition_dir}.zip", partition
            )

    # Now that each partition is zipped, remove the directories
    for partition in dir_partitions:
        partition_dir = f"partitions/{partition}"
        remove(partition_dir)

    # Finally, upload to S3
    upload_directory_to_s3(
        "partitions",
        "ayman-extract",
        f"partitions/partitions{num_partitions}_1100MB_zip",
    )

    # Remove the zip files
    for partition in dir_partitions:
        partition_dir = f"partitions/{partition}.zip"
        remove(partition_dir)
