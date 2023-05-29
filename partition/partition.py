from casacore.tables import table
import os
import numpy as np

from casacore.tables import table
import os
import numpy as np

class Partitioner:
    def __init__(self, input_set):
        self.input_set = input_set

    def partition(self, duration):
        t = table(self.input_set, readonly=False)
        t = t.sort("TIME")  # Sort the table based on the time column
        num_rows = len(t)
        times = t.getcol("TIME")

        os.makedirs('partitions', exist_ok=True)
        partitions = []

        start_time = times[0]
        end_time = start_time + duration
        start_index = 0

        for i in range(num_rows):
            current_time = times[i]
            if current_time >= end_time:
                partition = t.selectrows(np.arange(start_index, i))  # Select rows up to but not including current row
                print(f'Partitioning rows {start_index} to {i} into {partition.nrows()} rows')
                partition_name = f'partitions/partition_{len(partitions)+1}.ms'
                partition.copy(partition_name, deep=True)
                partitions.append(partition_name)

                partition.close()

                start_time = current_time
                end_time = start_time + duration
                start_index = i  # Start next partition at current row

            if i % 100000 == 0:  # Print a progress update every 100,000 rows
                print(f'Processed {i} rows')

        # Handle last partition
        partition = t.selectrows(np.arange(start_index, num_rows))  # Select remaining rows
        print(f'Partitioning rows {start_index} to {num_rows} into {partition.nrows()} rows')
        partition_name = f'partitions/partition_{len(partitions)+1}.ms'
        partition.copy(partition_name, deep=True)
        partitions.append(partition_name)
        
        partition.close()
        t.close()

        return partitions
    
    def check_partition(self):
        original_t = table(self.original_set, readonly=True)
        partitioned_t = table(self.partitioned_set, readonly=True)

        original_times = original_t.getcol("TIME")
        partitioned_times = partitioned_t.getcol("TIME")

        # Check if all times in partitioned set are in the original set
        is_part_of_original = np.all(np.isin(partitioned_times, original_times))

        # Get the row indices of the partitioned set in the original set
        partitioned_indices = np.where(np.isin(original_times, partitioned_times))[0]

        original_t.close()
        partitioned_t.close()

        return is_part_of_original, partitioned_indices

class PartitionChecker:
    def __init__(self, original_set, partitioned_set):
        self.original_set = original_set
        self.partitioned_set = partitioned_set

    def check_partition(self):
        original_t = table(self.original_set, readonly=True)
        partitioned_t = table(self.partitioned_set, readonly=True)

        original_times = original_t.getcol("TIME")
        partitioned_times = partitioned_t.getcol("TIME")

        # Check if all times in partitioned set are in the original set
        is_part_of_original = np.all(np.isin(partitioned_times, original_times))

        # Get the row indices of the partitioned set in the original set
        partitioned_indices = np.where(np.isin(original_times, partitioned_times))[0]

        original_t.close()
        partitioned_t.close()

        return is_part_of_original, partitioned_indices
    
if __name__ == "__main__":
    checker = PartitionChecker('/mnt/d/SB205.MS/', 'partitions/partition_1.ms')
    is_part_of_original, partitioned_indices = checker.check_partition()

    print(f'Is partition part of original: {is_part_of_original}')
    print(f'Rows in original set: {partitioned_indices}')