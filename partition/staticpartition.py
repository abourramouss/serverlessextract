from partitionstrategy import PartitionStrategy
from casacore.tables import table
import numpy as np

"""Given a ms, partition it from object storage so each partition is ingested by a single worker."""


class StaticPartition(PartitionStrategy):
    # Iconvinient: Each lambda should download the entire ms. This is not ideal.
    # Solutions:
    # 1. Use a different partitioning strategy i.e dynamic partitioning
    # 2. Use a bigger worker i.e batch
    # 3. Find if there is a way to index the ms so that each worker can download only the relevant rows (but from casacore)
    def partition(self, ms: str, num_chunks: int) -> list[str]:
        t = table(ms, readonly=False)
        t = t.sort(
            "OBSERVATION_ID"
        )  # Sort the table based on the observation id column
        num_rows = len(t)

        # Calculate the number of rows per chunk
        rows_per_chunk = num_rows // num_chunks

        for i in range(num_chunks):
            start_index = i * rows_per_chunk
            end_index = start_index + rows_per_chunk

            if i == num_chunks - 1:
                end_index = num_rows

            partition = t.selectrows(np.arange(start_index, end_index))

            partition_name = f"partitions/partition_{i}.ms"
            partition.copy(partition_name, deep=True)

            partition.close()

            print(
                f"Partitioned rows {start_index} to {end_index} into {partition.nrows()} rows"
            )
