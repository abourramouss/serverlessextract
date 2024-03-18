import lithops
import pickle
import time
import os
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from s3path import S3Path
from profiling import profiling_context, Job, detect_runtime_environment


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
    def execute_step(self, ms: S3Path, parameters: str, output_ms: S3Path):
        pass

    def _execute_step(self, id, *args, **kwargs):
        # Call the context manager, and it will start the profiler in a separate process
        print(f"Worker {id} executing step")
        if "args" in kwargs and isinstance(kwargs["args"], tuple):
            command_args = kwargs["args"]
        else:
            raise ValueError("Expected 'args' key with a tuple value in kwargs")
        # Yields a profiler object, creates a new process for profiling.
        with profiling_context(os.getpid()) as profiler:
            function_timers = self.execute_step(*command_args)
        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()

        print(f"Worker {id} finished step" f" on {env} instance {instance_type}")
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def run(
        self,
        chunk_size: int,
        runtime_memory: int,
        cpus_per_worker: int,
        func_limit: Optional[int] = None,
    ):
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(runtime_memory=runtime_memory)
        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path.bucket,
            prefix=f"{self.input_data_path.key}/",
        )
        if f"{self.input_data_path.key}/" in keys:
            keys.remove(f"{self.input_data_path.key}/")
        if func_limit:
            keys = keys[:func_limit]

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

        start_time = time.time()
        futures = function_executor.map(
            self._execute_step, s3_paths, extra_env=extra_env
        )

        results = function_executor.get_result(futures)
        end_time = time.time()

        env = results[0]["env"]
        instance_type = results[0]["instance_type"]
        profilers = [result["profiler"] for result in results]

        for result, future in zip(results, futures):
            profiler = result["profiler"]
            profiler.worker_start_tstamp = future.stats["worker_start_tstamp"]
            profiler.worker_end_tstamp = future.stats["worker_end_tstamp"]

        job = Job(
            memory=runtime_memory,
            cpus_per_worker=cpus_per_worker,
            chunk_size=chunk_size,
            start_time=start_time,
            end_time=end_time,
            number_workers=len(profilers),
            profilers=profilers,
            instance_type=instance_type,
            environment=env,
        )

        return job
