import lithops
import pickle
import time
import os
import subprocess
import logging
import subprocess as sp
import pprint

from abc import ABC, abstractmethod
from typing import Dict, List, Union, Optional
from s3path import S3Path
from pathlib import PosixPath
from profiling import profiling_context, Job, detect_runtime_environment
from datasource import (
    LithopsDataSource,
    InputS3,
    OutputS3,
    s3_to_local_path,
    local_path_to_s3,
)
from util import dict_to_parset

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


class DP3Step:
    def __init__(self, parameters: Union[Dict, List[Dict]]):
        if isinstance(parameters, dict):
            self._parameters = [parameters]
        else:
            self._parameters = parameters

    def execute_step(self, parameters: bytes, id):
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()

        params = pickle.loads(parameters)
        logger.info(f"Worker id: {id} started execution with parameters: {params}")

        directories = {}
        for key, val in params.items():
            if isinstance(val, InputS3):
                logger.info(f"Downloading data for key {key} from S3: {val}")
                path = data_source.download_directory(val, working_dir)
                logger.info(
                    f"Downloaded path type: {'Directory' if path.is_dir() else 'File'} at {path}"
                )

                logger.info(
                    f"Checking path: {path}, Type: {type(path)}, Exists: {path.exists()}, Is File: {path.is_file()}"
                )

                if path.is_dir():
                    logger.info(f"Path {path} is a directory")
                elif path.is_file():
                    logger.info(f"Path {path} is a file with extension {path.suffix}")
                    if path.suffix.lower() == ".zip":
                        logger.info(f"Extracting zip file at {path}")
                        path = data_source.unzip(path)
                    else:
                        logger.info(f"Path {path} is a recognized file type.")
                else:
                    # TODO: Handle case where h5 file isn't related to msin
                    logger.warning(
                        f"Path {path} is neither a directory nor a recognized file type."
                    )
                    logger.info(
                        f"File status - Exists: {os.path.exists(path)}, Is File: {os.path.isfile(path)}"
                    )

                params[key] = str(path)

            elif isinstance(val, OutputS3):
                logger.info(f"Preparing output path for key {key} using {val}")
                local_directory_path = s3_to_local_path(val, base_local_dir=working_dir)
                final_output_path = (
                    local_directory_path / f"{val.file_name}.{val.file_ext}"
                )
                os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
                logger.info(f"Output path prepared: {final_output_path}")
                # TODO: Add the parameters or whatever at the end, basically compose the key correctly
                directories[final_output_path] = val
                params[key] = str(final_output_path)
                logger.info(f"Directories {directories}")
        logger.info(f"Final params for DP3 command: {params}")
        params_path = dict_to_parset(params)
        cmd = ["DP3", str(params_path)]
        logger.info(f"Executing DP3 command with parameters: {cmd}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        logger.info(f"DP3 execution stdout: {stdout if stdout else 'No Output'}")
        logger.info(f"DP3 execution stderr: {stderr if stderr else 'No Errors'}")

        # TODO: Remove directories dict, it can be only an array
        for key, val in directories.items():
            logger.info(f"Checking existence of output path: {key}")
            if os.path.exists(key):
                logger.info(f"Path exists, proceeding to zip: {key}")
                try:
                    logger.info(f"Going to zip {key}")
                    zip_path = data_source.zip_without_compression(key)

                    logger.info(f"Zip created at {zip_path}, uploading to S3")
                    data_source.upload_file(zip_path, local_path_to_s3(zip_path))
                    logger.info(
                        f"Uploaded zip file to S3: {local_path_to_s3(zip_path)}"
                    )
                except IsADirectoryError as e:
                    logger.error(f"Error while zipping: {e}")
            else:
                # Do something else in here
                logger.error(f"Path {key} does not exist. Skipping zipping.")

        time_records = []
        logger.info(
            f"Worker id: {id} completed execution. Time records: {time_records}"
        )
        return time_records

    def _execute_step(self, id, *args, **kwargs):
        memory_limit = get_memory_limit_cgroupv2()
        cpu_limit = get_cpu_limit_cgroupv2()

        logger.info(f"Memory Limit: {memory_limit} GB")
        logger.info(f"CPU Limit: {cpu_limit}")
        logger.info(f"Worker {id} executing step")
        logger.info(f"kwargs: {kwargs} args: {args}")
        if "args" in kwargs and isinstance(kwargs["args"], tuple):
            command_args = kwargs["args"]
        else:
            raise ValueError("Expected 'args' key with a tuple value in kwargs")

        list_of_args = pickle.loads(command_args)

        for arg in list_of_args:
            logger.info(f"argument: {arg}")
        with profiling_context(os.getpid()) as profiler:
            logger.info(f"commands args: {command_args}")
            for arg in command_args:
                logger.info(f"argument: {arg}")
            function_timers = []  # self.execute_step(*command_args, id=id)

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
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
            log_level="INFO",
        )

        # Use the first set's 'msin' for determining the bucket and prefix
        bucket = self._parameters[0]["msin"].bucket
        prefix = self._parameters[0]["msin"].key
        keys = lithops.Storage().list_keys(bucket=bucket, prefix=prefix)
        if func_limit:
            keys = keys[:func_limit]

        chunk_size = f"{int(lithops.Storage().head_object(bucket, keys[0])['content-length']) / 1024 ** 2} MB"

        function_params = []
        for key in keys:
            key_name = key.split("/")[-1].split(".")[0]
            for params in self._parameters:
                new_params = params.copy()
                new_params["msin"] = InputS3(
                    bucket=bucket, key=key
                )  # Update msin for all parameter sets
                for k, v in new_params.items():
                    if isinstance(v, OutputS3):
                        new_params[k] = OutputS3(
                            bucket=v.bucket,
                            key=f"{v.key}",
                            file_ext=v.file_ext,
                            file_name=key_name,
                        )
                    elif isinstance(v, InputS3) and v.dynamic:
                        dynamic_key_prefix = f"{v.key}/{key_name}"
                        dynamic_keys = lithops.Storage().list_keys(
                            bucket=v.bucket, prefix=dynamic_key_prefix
                        )
                        if len(dynamic_keys) == 1:
                            new_params[k] = InputS3(
                                bucket=v.bucket, key=dynamic_keys[0], dynamic=True
                            )
                        elif len(dynamic_keys) > 1:
                            raise Exception(
                                "Multiple keys found for a supposed unique dynamic path."
                            )
                        else:
                            raise Exception(
                                "No valid key found for dynamic path prefix."
                            )

                formatted_params = pprint.pformat(new_params, indent=4)
                logger.info(f"Parameters for key {key_name}: \n{formatted_params}")
                function_params.append(new_params)

        logger.info("Processed keys: {}".format([key for key in keys]))
        logger.info("Function parameters prepared for execution.")

        params_to_send = pickle.dumps(function_params)
        start_time = time.time()
        futures = function_executor.map(
            self._execute_step, params_to_send, extra_env=extra_env
        )
        results = function_executor.get_result(futures)
        end_time = time.time()

        job = Job(
            memory=runtime_memory,
            cpus_per_worker=cpus_per_worker,
            chunk_size=chunk_size,
            start_time=start_time,
            end_time=end_time,
            number_workers=len(results),
            profilers=[result.get("profiler", None) for result in results],
            instance_type=results[0].get("instance_type", "unknown"),
            environment=results[0].get("env", "unknown"),
        )

        return job


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
