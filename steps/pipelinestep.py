import lithops
import pickle
import time
import os
import subprocess
import subprocess as sp
import pprint

from typing import Dict, List, Union, Optional
from pathlib import PosixPath
from profiling import profiling_context, Job, detect_runtime_environment
from datasource import (
    LithopsDataSource,
    InputS3,
    OutputS3,
    s3_to_local_path,
    local_path_to_s3,
)
from util import dict_to_parset, setup_logging


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
    def __init__(self, parameters: List[Dict], log_level):
        if isinstance(parameters, dict):
            self._parameters = [parameters]
        else:
            self._parameters = parameters
        self._log_level = log_level
        self._logger = setup_logging(self._log_level)
        self._logger.debug("DP3 Step initialized")

    def execute_step(self, parameters: bytes, id=None):
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()

        params = pickle.loads(parameters)
        self._logger.info(
            f"Worker id: {id} started execution with parameters: {params}"
        )

        directories = {}
        for key, val in params.items():
            if isinstance(val, InputS3):
                self._logger.info(f"Downloading data for key {key} from S3: {val}")
                path = data_source.download_directory(val, working_dir)
                self._logger.debug(
                    f"Downloaded path type: {'Directory' if path.is_dir() else 'File'} at {path}"
                )

                self._logger.debug(
                    f"Checking path: {path}, Type: {type(path)}, Exists: {path.exists()}, Is File: {path.is_file()}"
                )

                if path.is_dir():
                    self._logger.info(f"Path {path} is a directory")
                elif path.is_file():
                    self._logger.info(
                        f"Path {path} is a file with extension {path.suffix}"
                    )
                    if path.suffix.lower() == ".zip":
                        path = data_source.unzip(path)
                        self._logger.debug(f"Extracting zip file at {path}")
                    else:
                        self._logger.debug(f"Path {path} is a recognized file type.")
                else:
                    # TODO: Handle case where h5 file isn't related to msin
                    self._logger.warning(
                        f"Path {path} is neither a directory nor a recognized file type."
                    )
                    self._logger.debug(
                        f"File status - Exists: {os.path.exists(path)}, Is File: {os.path.isfile(path)}"
                    )

                params[key] = str(path)

            elif isinstance(val, OutputS3):
                self._logger.info(f"Preparing output path for key {key} using {val}")
                local_directory_path = s3_to_local_path(val, base_local_dir=working_dir)
                final_output_path = (
                    local_directory_path / f"{val.file_name}.{val.file_ext}"
                )
                os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
                self._logger.debug(f"Output path prepared: {final_output_path}")
                # TODO: Add the parameters or whatever at the end, basically compose the key correctly
                directories[final_output_path] = val
                params[key] = str(final_output_path)
                self._logger.debug(f"Directories {directories}")
        self._logger.debug(f"Final params for DP3 command: {params}")
        params_path = dict_to_parset(params)
        cmd = ["DP3", str(params_path)]
        self._logger.debug(f"Executing DP3 command with parameters: {cmd}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        self._logger.info(f"DP3 execution stdout: {stdout if stdout else 'No Output'}")
        self._logger.info(f"DP3 execution stderr: {stderr if stderr else 'No Errors'}")

        # TODO: Remove directories dict, it can be an array
        for key, val in directories.items():
            self._logger.debug(f"Checking existence of output path: {key}")
            if os.path.exists(key):
                self._logger.debug(f"Path exists, proceeding to zip: {key}")
                try:
                    self._logger.debug(f"Going to zip {key}")
                    zip_path = data_source.zip_without_compression(key)

                    self._logger.debug(f"Zip created at {zip_path}, uploading to S3")
                    data_source.upload_file(zip_path, local_path_to_s3(zip_path))
                    self._logger.debug(
                        f"Uploaded zip file to S3: {local_path_to_s3(zip_path)}"
                    )
                except IsADirectoryError as e:
                    self._logger.error(f"Error while zipping: {e}")
            else:
                # Do something else in here
                self._logger.error(f"Path {key} does not exist. Skipping zipping.")

        time_records = []
        self._logger.info(
            f"Worker id: {id} completed execution. Time records: {time_records}"
        )
        return time_records

    def _execute_step(self, id, command_args, *args, **kwargs):
        self._logger = setup_logging(self._log_level)
        memory_limit = get_memory_limit_cgroupv2()
        cpu_limit = get_cpu_limit_cgroupv2()

        self._logger.info(f"Memory Limit: {memory_limit} GB")
        self._logger.info(f"CPU Limit: {cpu_limit}")
        self._logger.info(f"Worker {id} executing step")
        self._logger.info(f"Command args: {command_args}")

        with profiling_context(os.getpid()) as profiler:
            for params in command_args:
                self._logger.debug(f"Processing params: {params}")
                params = pickle.dumps(params)
                self._logger.debug(f"Serialized params: {params}")
                self.execute_step(params, id=id)
            function_timers = []

        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()
        self._logger.info(
            f"Worker {id} finished step on {env} instance {instance_type}"
        )
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def run(self, func_limit: Optional[int] = None):
        runtime_memory = 4000
        cpus_per_worker = 2
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
            log_level=self._log_level,
        )

        # Get bucket and prefix from the first set of parameters
        bucket = self._parameters[0]["msin"].bucket
        prefix = self._parameters[0]["msin"].key
        keys = lithops.Storage().list_keys(bucket=bucket, prefix=prefix)
        if func_limit:
            keys = keys[:func_limit]

        self._logger.debug(keys)
        chunk_size = f"{int(lithops.Storage().head_object(bucket, keys[0])['content-length']) / 1024 ** 2} MB"

        grouped_params = {}
        for key in keys:
            partition_name = key.split("/")[-1].split(".")[0]
            key_name = key.split("/")[-1]
            all_params_for_key = []
            for params in self._parameters:
                new_params = params.copy()
                new_key = (
                    f"{params['msin'].key}/{key_name}"
                    if not params["msin"].key.endswith("/")
                    else f"{params['msin'].key}{key_name}"
                )
                new_params["msin"] = InputS3(bucket=bucket, key=new_key)
                for k, v in new_params.items():
                    if isinstance(v, OutputS3):
                        new_params[k] = OutputS3(
                            bucket=v.bucket,
                            key=f"{v.key}",
                            file_ext=v.file_ext,
                            file_name=partition_name,
                        )
                    elif isinstance(v, InputS3) and v.dynamic:
                        dynamic_key_prefix = f"{v.key}/{partition_name}.{v.file_ext}"
                        new_params[k] = InputS3(
                            bucket=v.bucket,
                            key=dynamic_key_prefix,
                        )

                all_params_for_key.append(new_params)

            grouped_params[key] = all_params_for_key

        function_params = [grouped_params[key] for key in keys]
        for params in function_params:
            for new_params in params:
                formatted_params = pprint.pformat(new_params, indent=4)
                self._logger.debug(
                    f"Parameters for key {key_name}: \n{formatted_params}"
                )

        start_time = time.time()
        futures = function_executor.map(
            self._execute_step, function_params, extra_env=extra_env
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
