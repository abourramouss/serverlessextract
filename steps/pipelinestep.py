import lithops
import pickle
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from s3path import S3Path


class PipelineStep(ABC):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters: Dict,
        output: Optional[Dict[str, S3Path]] = None,
    ):
        self._input_data_path = input_data_path
        self._parameters = parameters
        self._output = output

    @property
    @abstractmethod
    def input_data_path(self) -> List[S3Path]:
        pass

    @property
    @abstractmethod
    def parameters(self) -> Dict:
        pass

    @property
    @abstractmethod
    def output(self) -> S3Path:
        pass

    @abstractmethod
    def build_command(self, *args, **kwargs):
        pass

    def run(self, func_limit: int):
        extra_env = {"HOME": "/tmp"}
        function_executor = lithops.FunctionExecutor()

        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path.bucket,
            prefix=f"{self.input_data_path.key}/",
        )
        print(f"{self.input_data_path.key}/")
        # Create an empty set to hold unique directories
        unique_partitions = set()

        partition_subset = 0

        # Iterate over each key
        for key in keys:
            # Split the key into its parts
            parts = key.split("/")
            # Extract the directory that ends in .ms
            partition = next((part for part in parts if part.endswith(".ms")), None)
            if partition and partition_subset < func_limit:
                # Combine the prefix with the partition

                full_partition_path = "/".join(parts[: parts.index(partition) + 1])
                unique_partitions.add(full_partition_path)
                partition_subset = len(unique_partitions)

        s3_paths = {
            (
                S3Path.from_bucket_key(
                    bucket=self.input_data_path.bucket, key=partition
                ),
                pickle.dumps(self.parameters),
                self.output,
            )
            for partition in unique_partitions
        }
        futures = function_executor.map(
            self.build_command,
            s3_paths,
            extra_env=extra_env,
        )
        results = function_executor.get_result(futures)
        return results
