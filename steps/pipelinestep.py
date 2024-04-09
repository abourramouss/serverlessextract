import lithops
import pickle
import time
import os
import subprocess
import logging

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from s3path import S3Path
from profiling import profiling_context, Job, detect_runtime_environment


log_format = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d -- %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)
logger.propagate = True


def get_memory_limit_cgroupv2():
    try:
        output = (
            subprocess.check_output(["cat", "/sys/fs/cgroup/memory.max"])
            .decode("utf-8")
            .strip()
        )
        if output == "max":
            return "No limit"
        memory_limit_gb = int(output) / (1024**3)
        return memory_limit_gb
    except Exception as e:
        return str(e)


def get_cpu_limit_cgroupv2():
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            cpu_max = f.read().strip()
            quota, period = cpu_max.split(" ")
            quota = int(quota)
            period = int(period)

        if quota == -1:  # No limit
            return "No limit"
        else:
            cpu_limit = quota / period
            return cpu_limit
    except Exception as e:
        return str(e)


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
        memory_limit = get_memory_limit_cgroupv2()
        cpu_limit = get_cpu_limit_cgroupv2()

        logger.info(f"Memory Limit: {memory_limit} GB")
        logger.info(f"CPU Limit: {cpu_limit}")
        logger.info(f"Worker {id} executing step")

        if "args" in kwargs and isinstance(kwargs["args"], tuple):
            command_args = kwargs["args"]
        else:
            raise ValueError("Expected 'args' key with a tuple value in kwargs")

        with profiling_context(os.getpid()) as profiler:
            function_timers = self.execute_step(*command_args)
        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()
        logger.info(f"Worker {id} finished step on {env} instance {instance_type}")
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def run(self, func_limit: Optional[int] = None):
        # provisioning_layer = ProvisioningLayer()
        # previous_execution_data = self._collect_previous_execution_data()
        # optimal_parameters = provisioning_layer.get_optimal_parameters(
        #    self.input_data_path, previous_execution_data
        # )

        # Hardcoded optimal parameters for testing
        runtime_memory = 4000
        cpus_per_worker = 2

        # Partition the dataset based on the optimal number of partitions
        # partition_sizes = partition_ms(self.input_data_path, num_partitions)

        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
            log_level="INFO",
        )

        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path.bucket,
            prefix=f"{self.input_data_path.key}/",
        )

        # Get size of the first chunk in the list, that will be the chunk size
        chunk_size = f"{lithops.Storage().head_object(self.input_data_path.bucket, keys[0])['content-length']}/{1024**2}"

        if f"{self.input_data_path.key}/" in keys:
            keys.remove(f"{self.input_data_path.key}/")

        if func_limit:
            keys = keys[:func_limit]

        # Special treatment for list parameters
        if isinstance(self.parameters, list):
            serialized_parameters = [pickle.dumps(param) for param in self.parameters]
        else:
            serialized_parameters = pickle.dumps(self.parameters)

        s3_paths = [
            (
                S3Path.from_bucket_key(
                    bucket=self.input_data_path.bucket, key=partition
                ),
                serialized_parameters,
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
