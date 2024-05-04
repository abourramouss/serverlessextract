import logging
from radiointerferometry.utils import setup_logging
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.partitioning import StaticPartitioner

parameters = [
    {
        "msin": InputS3(bucket="ayman-extract", key="partitions/partitions_total_2_4/"),
        "num_partitions": 2,
        "msout": OutputS3(bucket="ayman-extract", key=f"partitions/"),
    }
]


log_level = logging.DEBUG

# Create an instance of StaticPartitioner
partitioner = StaticPartitioner(parameters, log_level)

# Execute the run method
result = partitioner.run()

# Print the result (optional)
print(result)
