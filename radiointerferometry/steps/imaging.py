import os
import lithops
import pickle
import subprocess as sp
import time

from pathlib import PosixPath
from typing import Dict, List, Optional
from s3path import S3Path
from radiointerferometry.datasource import (
    LithopsDataSource,
    InputS3,
    s3_to_local_path,
    local_path_to_s3,
)
from radiointerferometry.utils import detect_runtime_environment
from radiointerferometry.profiling import profiling_context, CompletedStep, Type, time_it
from radiointerferometry.utils import setup_logging


class ImagingStep:
    def __init__(
        self, input_data_path: Dict[str, InputS3], parameters: Dict, log_level
    ):
        self._input_data_path = input_data_path
        self._parameters = parameters
        self._log_level = log_level
        self._logger = setup_logging(self._log_level)
        self._logger.debug("DP3 Step initialized")

    def execute_step(self, ms: List[InputS3], parameters: bytes):
        self._logger = setup_logging(self._log_level)
        working_dir = PosixPath(os.getenv("HOME"))
        time_records = []
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        for param in params:
            # check -name parameter and do a s3_to_local_path
            if param == "-name":
                output_ms = params[params.index(param) + 1]
                posix_source = s3_to_local_path(output_ms)
                self._logger.debug(f"Posix source: {posix_source}")
                params[params.index(param) + 1] = str(posix_source)
                break

        self._logger.info(f"modified params: {params}")
        # Initialize an empty list to store partition paths
        partitions = []
        for partition in ms:
            self._logger.info("Partition: {partition}")
            partition_path = time_it(
                "download_ms",
                data_source.download_directory,
                Type.READ,
                time_records,
                partition,
                base_path=working_dir,
            )
            partition_path = time_it(
                "unzip", data_source.unzip, Type.READ, time_records, partition_path
            )

            partitions.append(str(partition_path))

        cmd = [
            "wsclean",
        ]

        cmd.extend(params)
        # Append the paths of all partitions to the command
        cmd.extend(partitions)

        self._logger.info(f"cmd: {cmd}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)

        stdout, stderr = proc.communicate()

        self._logger.info("stdout:")
        self._logger.info(stdout)
        self._logger.info("stderr:")
        self._logger.info(stderr)

        directory_path = os.path.dirname(posix_source)

        files_in_directory = os.listdir(directory_path)

        image = PosixPath(
            next(
                (
                    os.path.join(directory_path, file)
                    for file in files_in_directory
                    if file.endswith("-image.fits")
                ),
                None,
            )
        )

        self._logger.debug(f"image_dir: {image}")

        data_source.upload_file(
            image,
            local_path_to_s3(image),
        )

        return time_records

    def _execute_step(self, id, *args, **kwargs):
        ms = kwargs["kwargs"]["ms"]
        parameters = kwargs["kwargs"]["parameters"]

        self._logger.info(f"Worker executing step with {len(ms)} ms paths")

        # Call the actual execution step
        with profiling_context(os.getpid()) as profiler:
            function_timers = self.execute_step(ms, parameters)

        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()
        self._logger.info(f"Worker finished step on {env} instance {instance_type}")
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def run(
        self,
        func_limit: Optional[int] = None,
    ):
        # Parameters to optimize
        runtime_memory = 2000
        cpus_per_worker = 2
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
            log_level=self._log_level,
        )

        keys = lithops.Storage().list_keys(
            bucket=self._input_data_path.bucket,
            prefix=f"{self._input_data_path.key}/",
        )

        if f"{self._input_data_path.key}/" in keys:
            keys.remove(f"{self.input_data_path.key}/")
        if func_limit:
            keys = keys[:func_limit]

        ms = [
            S3Path.from_bucket_key(bucket=self._input_data_path.bucket, key=partition)
            for partition in keys
        ]

        chunk_size = f"{lithops.Storage().head_object(self._input_data_path.bucket, keys[0])['content-length']}/{1024**2}"

        self._parameters.extend(["-j", str(cpus_per_worker)])
        parameters = pickle.dumps(self._parameters)
        # append the number of cpus to parameters

        start_time = time.time()
        future = function_executor.call_async(
            func=self._execute_step,
            data={
                "args": [],
                "kwargs": {
                    "ms": ms,
                    "parameters": parameters,
                },
            },
            extra_env=extra_env,
        )

        self._logger.info(f"parameters: {self._parameters}")

        result = function_executor.get_result([future])

        end_time = time.time()

        try:
            env = result["env"]
            instance_type = result["instance_type"]
            profiler = result["profiler"]
        except KeyError as e:
            self._logger.error(
                f"KeyError: {e}. The expected key is not in the result dictionary."
            )

        profiler.worker_start_tstamp = future.stats["worker_start_tstamp"]
        profiler.worker_end_tstamp = future.stats["worker_end_tstamp"]



        """
        
        
        completed_step = CompletedStep(
            memory=runtime_memory,
            cpus_per_worker=cpus_per_worker,
            chunk_size=chunk_size,
            start_time=start_time,
            end_time=end_time,
            number_workers=1,
            profilers=[profiler],
            instance_type=instance_type,
            environment=env,
        )

        return completed_step
        """