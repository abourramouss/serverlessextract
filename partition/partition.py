from casacore.tables import table
import os
import numpy as np
from upload import upload_directory_to_s3
import shutil
import zipfile


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

    def partition_chunks(self, num_chunks):
        os.makedirs("partitions", exist_ok=True)
        partition_counter = 1
        for input_file in self.input_files:
            t = table(input_file, readonly=False)
            t = t.sort("TIME")  # Sort the table based on the time column
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
                    partition_times = np.array(partition.getcol("TIME"))

                    is_exact_subset = np.array_equal(
                        np.sort(partition_times), np.sort(original_times[start_index:i])
                    )
                    print(
                        f"Partition {partition_counter} is exact subset of original table slice? {is_exact_subset}"
                    )
                    print(
                        f"Partitioning rows {start_index} to {i} into {partition.nrows()} rows"
                    )

                    partition_name = f"partitions/partition_{partition_counter}.ms"
                    partition.copy(partition_name, deep=True)
                    partition.close()

                    start_time = current_time
                    end_time = start_time + chunk_duration
                    start_index = i
                    partition_counter += 1

                    break  # Break after the first partition is created

                if i % 100000 == 0:
                    print(f"Processed {i} rows")

            t.close()

        return partition_counter - 1


if __name__ == "__main__":
    partitions = [61, 30, 15, 7, 3, 2]
    for pr in partitions:
        p = Partitioner("/home/ayman/Downloads/SB205.MS")
        total_partitions = p.partition_chunks(pr)
        print(f"Total partitions created: {total_partitions+1}")
        # List the partition directories after partitioning is complete
        dir_partitions = os.listdir("partitions")
        # Zip each partition directory
        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}"
            if os.path.isdir(partition_dir):  # Make sure it's a directory
                zip_directory_without_compression(
                    partition_dir, f"{partition_dir}.zip", partition
                )

        # Now that each partition is zipped, you can remove the directories
        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}"
            remove(partition_dir)

        # Finally, upload to S3
        upload_directory_to_s3(
            "partitions",
            "ayman-extract",
            f"partitions/partitions_{pr}zip",
        )

        # Remove the zip files
        for partition in dir_partitions:
            partition_dir = f"partitions/{partition}.zip"
            remove(partition_dir)
