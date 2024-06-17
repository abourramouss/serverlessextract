import lithops
import time
import os
import subprocess as sp
import copy

from typing import Dict, List, Optional
from pathlib import PosixPath

from radiointerferometry.profiling import (
    profiling_context,
    CompletedStep,
    Type,
    time_it,
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
    detect_runtime_environment,
    get_memory_limit_cgroupv2,
    get_cpu_limit_cgroupv2,
    get_executor_id_lithops,
)


class DP3Step:
    def __init__(self, parameters: List[Dict], log_level):
        if isinstance(parameters, dict):
            self.__parameters = [parameters]
        else:
            self.__parameters = parameters
        self.__log_level = log_level
        self.__logger = setup_logging(self.__log_level)
        self.__logger.debug("DP3 Step initialized")

    def __call__(
        self, func_limit: Optional[int] = None, step_name: Optional[str] = None
    ):
        return self.run(func_limit=func_limit, step_name=step_name)

    def execute_step(self, params: dict, id=None):
        time_records = []
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()

        self.__logger.info(
            f"Worker id: {id} started execution with parameters: {params}"
        )

        directories = {}
        for key, val in params.items():
            if isinstance(val, InputS3):
                self.__logger.info(f"Downloading data for key {key} from S3: {val}")

                path = time_it(
                    "Download directory",
                    data_source.download_directory,
                    Type.READ,
                    time_records,
                    val,
                    working_dir,
                )
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
                        # Timing the unzip
                        path = time_it(
                            "Unzip file",
                            data_source.unzip,
                            Type.READ,
                            time_records,
                            path,
                        )
                        self.__logger.debug(f"Extracting zip file at {path}")
                        unzipped_contents = os.listdir(path)
                        self.__logger.info(
                            f"Contents of {path} after unzip: {unzipped_contents}"
                        )
                        # Sometimes a zip file contains a single directory with the contents, in which case we adjust the path
                        if len(unzipped_contents) == 1 and os.path.isdir(
                            os.path.join(path, unzipped_contents[0])
                        ):
                            path = os.path.join(path, unzipped_contents[0])
                            self.__logger.info(f"Adjusted path after unzip: {path}")
                    else:
                        self.__logger.debug(f"Path {path} is a recognized file type.")

                params[key] = str(path)

            elif isinstance(val, OutputS3):
                self.__logger.info(f"Preparing output path for key {key} using {val}")
                local_directory_path = s3_to_local_path(val, base_local_dir=working_dir)
                os.makedirs(local_directory_path.parent, exist_ok=True)
                final_output_path = local_directory_path
                self.__logger.debug(f"Output path prepared: {final_output_path}")
                directories[final_output_path] = val
                params[key] = str(final_output_path)

        self.__logger.debug(f"Final params for DP3 command: {params}")
        params_path = dict_to_parset(params)
        cmd = ["DP3", str(params_path)]
        self.__logger.debug(f"Executing DP3 command with parameters: {cmd}")
        log_output = params["log_output"]

        stdout, stderr = time_it(
            "Execute DP3 command",
            self.run_command,
            Type.COMPUTE,
            time_records,
            cmd,
            log_output,
        )

        self.__logger.info(f"DP3 execution log saved to {params['log_output']}")
        self.__logger.info(f"DP3 execution stdout: {stdout if stdout else 'No Output'}")
        self.__logger.info(f"DP3 execution stderr: {stderr if stderr else 'No Errors'}")

        for key, val in directories.items():
            if os.path.exists(key):
                self.__logger.debug(f"Path exists, proceeding to zip: {key}")
                try:
                    zip_path = time_it(
                        "Zip without compression",
                        data_source.zip_without_compression,
                        Type.WRITE,
                        time_records,
                        key,
                    )
                    time_it(
                        "Upload file",
                        data_source.upload_file,
                        Type.WRITE,
                        time_records,
                        zip_path,
                        local_path_to_s3(zip_path),
                    )
                except IsADirectoryError as e:
                    self.__logger.error(f"Error while zipping: {e}")

        self.__logger.debug(
            f"Worker id: {id} completed execution. Time records: {time_records}"
        )
        return time_records

    def _execute_step(self, id, parameter_list: List[Dict]):
        self.__logger = setup_logging(self.__log_level)
        memory_limit = get_memory_limit_cgroupv2()
        cpu_limit = get_cpu_limit_cgroupv2()
        msin = parameter_list[0]["msin"]
        chunk_size = round(
            int(lithops.Storage().head_object(msin.bucket, msin.key)["content-length"])
            / 1024**2,
            2,
        )

        self.__logger.info(f"Memory Limit: {memory_limit} GB")
        self.__logger.info(f"CPU Limit: {cpu_limit}")
        self.__logger.info(f"Worker {id} executing step")
        self.__logger.info(f"parameter list: {parameter_list}")

        with profiling_context(os.getpid()) as profiler:
            function_timers = []
            for param in parameter_list:
                timers = self.execute_step(param, id=id)
                function_timers.extend(timers)

        profiler.worker_id = id
        profiler.worker_chunk_size = chunk_size
        profiler.worker_ingested_key = parameter_list[0]["msin"]
        profiler.function_timers = function_timers

        env, instance_type = detect_runtime_environment()
        self.__logger.info(
            f"Worker {id} finished step on {env} instance {instance_type}"
        )

        self.__logger.info(f"_execute_step id{id}")

        for key, value in os.environ.items():
            self.__logger.info(f"{key}={value}")
        self.__logger.info("ENV VARIABLES")
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def __construct_params_for_key(self, base_params, key, bucket):
        new_params = copy.deepcopy(base_params)
        file_name_suffix = key.split("/")[-1].split(".")[0]
        new_params["msin"] = InputS3(bucket=bucket, key=key)

        for k, v in new_params.items():
            if isinstance(v, OutputS3):
                new_file_name = (
                    f"{file_name_suffix}.{v.file_ext if v.file_ext else 'default_ext'}"
                )
                new_key_path = f"{v.key}/{new_file_name}"
                new_params[k] = OutputS3(
                    bucket=v.bucket,
                    key=new_key_path,
                )
                self.__logger.info(f"New output path: {new_key_path}")
            elif isinstance(v, InputS3) and v.dynamic:
                dynamic_key = f"{v.key}/{file_name_suffix}.{v.file_ext}"
                new_params[k] = InputS3(bucket=bucket, key=dynamic_key)

        return new_params

    def run_command(self, cmd, log_output):
        with open(log_output, "w") as log_file:
            proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
            stdout, stderr = proc.communicate()
            log_file.write(f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        return stdout, stderr

    def run(self, func_limit: Optional[int] = None, step_name: Optional[str] = None):
        runtime_memory = 500
        cpus_per_worker = 1
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}

        lithops_fexec_parameters = {"log_level": self.__log_level}

        function_executor = lithops.FunctionExecutor(
            log_level=self.__log_level,
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
        )

        bucket = self.__parameters[0]["msin"].bucket
        prefix = self.__parameters[0]["msin"].key

        keys = lithops.Storage().list_keys(bucket=bucket, prefix=prefix)[:func_limit]

        self.__logger.info(f"keys : {keys}")

        step_ingested_size = sum(
            round(
                int(lithops.Storage().head_object(bucket, key)["content-length"])
                / 1024**2,
                2,
            )
            for key in keys
        )

        ingested_data = 0

        function_params = [
            [
                self.__construct_params_for_key(params, key, bucket)
                for params in self.__parameters
            ]
            for key in keys
        ]

        self.__logger.info(
            f"Function params: {function_params} and length {len(function_params)}"
        )
        start_time = time.time()

        futures = function_executor.map(
            self._execute_step, function_params, extra_env=extra_env
        )
        profiled_workers = function_executor.get_result(futures)
        end_time = time.time()
        step_cost = 0
        profilers = []
        for worker_id in range(len(profiled_workers)):
            profiled_workers[worker_id]["profiler"].worker_start_tstamp = futures[
                worker_id
            ].stats["worker_start_tstamp"]
            profiled_workers[worker_id]["profiler"].worker_end_tstamp = futures[
                worker_id
            ].stats["worker_end_tstamp"]

            profiled_workers[worker_id]["profiler"].worker_cold_start = (
                futures[worker_id].stats["worker_start_tstamp"]
                - futures[worker_id].stats["host_submit_tstamp"]
            )
            worker_duration = (
                futures[worker_id].stats["worker_end_tstamp"]
                - futures[worker_id].stats["worker_start_tstamp"]
            )
            aws_lambda_cost_per_ms_mb = 0.0000000167
            worker_cost = (
                worker_duration
                * 1000
                * aws_lambda_cost_per_ms_mb
                * (runtime_memory / 1024)
            )
            profiled_workers[worker_id]["profiler"].worker_cost = worker_cost

            profilers.append(profiled_workers[worker_id]["profiler"])
            step_cost += worker_cost
            ingested_data += profiled_workers[worker_id]["profiler"].worker_chunk_size
            write = 0
            compute = 0
            read = 0
            for timing in profiled_workers[worker_id]["profiler"].function_timers:
                if timing.operation_type == Type.WRITE:
                    write += timing.duration
                elif timing.operation_type == Type.COMPUTE:
                    compute += timing.duration
                elif timing.operation_type == Type.READ:
                    read += timing.duration

        # assertion: sum of keys sizes ingested by the workers is the same as step_ingested_size.
        assert step_ingested_size == ingested_data

        completed_step = CompletedStep(
            step_id=get_executor_id_lithops(),
            total_write_time=write,
            total_compute_time=compute,
            total_read_time=read,
            step_name=step_name,
            step_ingested_size=step_ingested_size,
            step_cost=step_cost,
            memory=runtime_memory,
            cpus_per_worker=cpus_per_worker,
            start_time=start_time,
            end_time=end_time,
            number_workers=len(profilers),
            profilers=profilers,
            instance_type=profiled_workers[0].get("instance_type", "unknown"),
            environment=profiled_workers[0].get("env", "unknown"),
        )

        return completed_step
