import lithops
import pickle
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from s3path import S3Path
import time
from util import profiling_context


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
    def build_command(self, ms: S3Path, parameters: str, output_ms: S3Path):
        pass

    def _execute_step(self, *args, **kwargs):
        # Call the context manager, and it will start the profiler in a separate process
        if "args" in kwargs and isinstance(kwargs["args"], tuple):
            command_args = kwargs["args"]
        else:
            raise ValueError("Expected 'args' key with a tuple value in kwargs")
        # Yields a profiler object, essentially what it does is create a new process for profiling, since multithreading wasn't working properly due to the GIL
        with profiling_context() as profiler:
            function_timers = self.build_command(*command_args)
        profiler.function_timers = function_timers
        return profiler

    def run(
        self, func_limit: Optional[int] = None, runtime_memory: Optional[int] = None
    ):
        extra_env = {"HOME": "/tmp"}

        function_executor = lithops.FunctionExecutor(runtime_memory=runtime_memory)
        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path.bucket,
            prefix=f"{self.input_data_path.key}/",
        )
        if f"{self.input_data_path.key}/" in keys:
            keys.remove(f"{self.input_data_path.key}/")
        if func_limit:
            keys = keys[0:func_limit]
        s3_paths = [
            (
                S3Path.from_bucket_key(
                    bucket=self.input_data_path.bucket, key=partition
                ),
                pickle.dumps(self.parameters),
                self.output,
            )
            for partition in keys
        ]

        futures = function_executor.map(
            self._execute_step, s3_paths, extra_env=extra_env
        )
        results = function_executor.get_result(futures)
        return results

    """
    This code is for ms as a directory, instead of zipping it.
    
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
    """
