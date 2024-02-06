import lithops
import pickle
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from s3path import S3Path
from pprint import pprint
from profiling import profiling_context


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

    def _execute_step(self, id, *args, **kwargs):
        # Call the context manager, and it will start the profiler in a separate process
        print(f"Worker {id} executing step")
        if "args" in kwargs and isinstance(kwargs["args"], tuple):
            command_args = kwargs["args"]
        else:
            raise ValueError("Expected 'args' key with a tuple value in kwargs")
        # Yields a profiler object, creates a new process for profiling.
        with profiling_context() as profiler:
            function_timers = self.build_command(*command_args)
        profiler.function_timers = function_timers
        profiler.worker_id = id
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

        # asociate futures worker_start_tstamp and worker_end_tstamp to the profiler via the profiler.worker_id and the futures position.
        # this way we wrap around customized profiling with lithops own stats for each worker.
        for result in results:
            worker_id = result.worker_id
            result.worker_start_tstamp = futures[worker_id].stats["worker_start_tstamp"]
            result.worker_end_tstamp = futures[worker_id].stats["worker_end_tstamp"]
        return results
