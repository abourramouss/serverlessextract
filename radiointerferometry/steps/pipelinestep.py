import lithops
import pickle
import time
import os
import subprocess as sp
import pprint

from typing import Dict, List, Optional
from pathlib import PosixPath
from radiointerferometry.profiling import (
    profiling_context,
    Job,
    detect_runtime_environment,
)
from radiointerferometry.datasource import (
    LithopsDataSource,
    InputS3,
    OutputS3,
    s3_to_local_path,
    local_path_to_s3,
)
from radiointerferometry.utils import (
    dict_to_parset,
    setup_logging,
    get_memory_limit_cgroupv2,
    get_cpu_limit_cgroupv2,
)
from flexexecutor import FlexExecutor


class DP3Step:
    def __init__(self, parameters: List[Dict], log_level):
        if isinstance(parameters, dict):
            self.__parameters = [parameters]
        else:
            self.__parameters = parameters
        self.__log_level = log_level
        self.__logger = setup_logging(self.__log_level)
        self.__logger.debug("DP3 Step initialized")

    def execute_step(self, params: dict, id=None):
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()

        self.__logger.info(
            f"Worker id: {id} started execution with parameters: {params}"
        )

        directories = {}
        for key, val in params.items():
            if isinstance(val, InputS3):
                self.__logger.info(f"Downloading data for key {key} from S3: {val}")
                path = data_source.download_directory(val, working_dir)
                self.__logger.debug(
                    f"Downloaded path type: {'Directory' if path.is_dir() else 'File'} at {path}"
                )

                self.__logger.debug(
                    f"Checking path: {path}, Type: {type(path)}, Exists: {path.exists()}, Is File: {path.is_file()}"
                )

                if path.is_dir():
                    self.__logger.info(f"Path {path} is a directory")
                elif path.is_file():
                    self.__logger.info(
                        f"Path {path} is a file with extension {path.suffix}"
                    )
                    if path.suffix.lower() == ".zip":
                        path = data_source.unzip(path)
                        self.__logger.debug(f"Extracting zip file at {path}")
                    else:
                        self.__logger.debug(f"Path {path} is a recognized file type.")
                else:
                    # TODO: Handle case where h5 file isn't related to msin
                    self.__logger.warning(
                        f"Path {path} is neither a directory nor a recognized file type."
                    )
                    self.__logger.debug(
                        f"File status - Exists: {os.path.exists(path)}, Is File: {os.path.isfile(path)}"
                    )

                params[key] = str(path)

            elif isinstance(val, OutputS3):
                self.__logger.info(f"Preparing output path for key {key} using {val}")
                local_directory_path = s3_to_local_path(val, base_local_dir=working_dir)
                final_output_path = (
                    local_directory_path / f"{val.file_name}.{val.file_ext}"
                )
                os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
                self.__logger.debug(f"Output path prepared: {final_output_path}")
                # TODO: Add the parameters or whatever at the end, basically compose the key correctly
                directories[final_output_path] = val
                params[key] = str(final_output_path)
                self.__logger.debug(f"Directories {directories}")
        self.__logger.debug(f"Final params for DP3 command: {params}")
        params_path = dict_to_parset(params)
        cmd = ["DP3", str(params_path)]
        self.__logger.debug(f"Executing DP3 command with parameters: {cmd}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        self.__logger.info(f"DP3 execution stdout: {stdout if stdout else 'No Output'}")
        self.__logger.info(f"DP3 execution stderr: {stderr if stderr else 'No Errors'}")

        # TODO: Remove directories dict, it can be an array
        for key, val in directories.items():
            self.__logger.debug(f"Checking existence of output path: {key}")
            if os.path.exists(key):
                self.__logger.debug(f"Path exists, proceeding to zip: {key}")
                try:
                    self.__logger.debug(f"Going to zip {key}")
                    zip_path = data_source.zip_without_compression(key)

                    self.__logger.debug(f"Zip created at {zip_path}, uploading to S3")
                    data_source.upload_file(zip_path, local_path_to_s3(zip_path))
                    self.__logger.debug(
                        f"Uploaded zip file to S3: {local_path_to_s3(zip_path)}"
                    )
                except IsADirectoryError as e:
                    self.__logger.error(f"Error while zipping: {e}")
            else:
                # Do something else in here
                self.__logger.error(f"Path {key} does not exist. Skipping zipping.")

        time_records = []
        self.__logger.info(
            f"Worker id: {id} completed execution. Time records: {time_records}"
        )
        return time_records

    def _execute_step(self, id, command_args):
        self.__logger = setup_logging(self.__log_level)
        memory_limit = get_memory_limit_cgroupv2()
        cpu_limit = get_cpu_limit_cgroupv2()

        self.__logger.info(f"Memory Limit: {memory_limit} GB")
        self.__logger.info(f"CPU Limit: {cpu_limit}")
        self.__logger.info(f"Worker {id} executing step")
        self.__logger.info(f"Command args: {command_args}")

        with profiling_context(os.getpid()) as profiler:
            self.execute_step(
                command_args, id=id
            )  # Call execute_step directly with command_args
            function_timers = []

        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()
        self.__logger.info(
            f"Worker {id} finished step on {env} instance {instance_type}"
        )
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def __construct_params_for_key(self, base_params, key, bucket):
        new_params = base_params.copy()
        new_key_suffix = key.split("/")[-1]
        new_key = f"{base_params['msin'].key.rstrip('/')}/{new_key_suffix}"
        new_params["msin"] = InputS3(bucket=bucket, key=new_key)

        for k, v in new_params.items():
            if isinstance(v, OutputS3):
                new_params[k] = OutputS3(
                    bucket=v.bucket,
                    key=f"{v.key}",
                    file_ext=v.file_ext,
                    file_name=key.split("/")[-1].split(".")[0],
                )
            elif isinstance(v, InputS3) and v.dynamic:
                dynamic_key = f"{v.key}/{new_key_suffix}.{v.file_ext}"
                new_params[k] = InputS3(bucket=v.bucket, key=dynamic_key)

        return {"command_args": new_params}

    def run(self, func_limit: Optional[int] = None):
        runtime_memory = 4000
        cpus_per_worker = 2
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            log_level=self.__log_level,
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
        )

        # Retrieve and limit keys if necessary
        bucket = self.__parameters[0]["msin"].bucket
        prefix = self.__parameters[0]["msin"].key
        keys = (
            lithops.Storage().list_keys(bucket=bucket, prefix=prefix)[:func_limit]
            if func_limit
            else lithops.Storage().list_keys(bucket=bucket, prefix=prefix)
        )

        self.__logger.debug(keys)
        chunk_size = f"{int(lithops.Storage().head_object(bucket, keys[0])['content-length']) / 1024 ** 2} MB"

        # Construct parameters for each key
        function_params = [
            self.__construct_params_for_key(params, key, bucket)
            for key in keys
            for params in self.__parameters
        ]

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
            profilers=[],
            instance_type=results[0].get("instance_type", "unknown"),
            environment=results[0].get("env", "unknown"),
        )

        return job
