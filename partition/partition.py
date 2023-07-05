from casacore.tables import table
import os
import numpy as np
from upload import upload_directory_to_s3
import shutil


def remove(path):
    """param <path> could either be relative or absolute."""
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))


# Class that takes a measurement set and partitions it into chunks, then uploads them to s3
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

            total_duration = (
                original_times[-1] - original_times[0]
            )  # Calculate total duration
            chunk_duration = total_duration / num_chunks  # Calculate chunk duration

            start_time = original_times[0]
            end_time = start_time + chunk_duration
            start_index = 0

            for i in range(num_rows):
                current_time = original_times[i]
                if current_time >= end_time:
                    partition = t.selectrows(np.arange(start_index, i))
                    partition_times = np.array(partition.getcol("TIME"))

                    # Check for an exact match in the elements and size between the partition and the slice from the original array
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
                    start_index = i  # Start next partition at current row
                    partition_counter += 1

                if i % 100000 == 0:  # Print a progress update every 100,000 rows
                    print(f"Processed {i} rows")

            if start_index < num_rows:
                partition = t.selectrows(np.arange(start_index, num_rows))
                partition_times = np.array(partition.getcol("TIME"))

                # Check for an exact match in the elements and size between the partition and the slice from the original array
                is_exact_subset = np.array_equal(
                    np.sort(partition_times), np.sort(original_times[start_index:])
                )
                print(
                    f"Partition {partition_counter} is exact subset of original table slice? {is_exact_subset}"
                )

                print(
                    f"Partitioning rows {start_index} to {num_rows} into {partition.nrows()} rows"
                )
                partition_name = f"partitions/partition_{partition_counter}.ms"
                partition.copy(partition_name, deep=True)

                partition.close()

            t.close()

        return partition_counter - 1


if __name__ == "__main__":
    partitions = 50
    p = Partitioner("/home/ayman/Downloads/entire_ms/SB205.MS/")
    total_partitions = p.partition_chunks(partitions)
    print(f"Total partitions created: {total_partitions}")

    upload_directory_to_s3(
        "partitions",
        "aymanb-serverless-genomics",
        f"extract-data/partitions_{partitions}",
    )

    # After uploading, delete the contents of the partitions directory
    dir_partitions = os.listdir("partitions")

    for dir in dir_partitions:
        remove(f"partitions/{dir}")
