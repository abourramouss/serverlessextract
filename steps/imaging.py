from pathlib import PosixPath
from typing import Dict, List, Optional
from s3path import S3Path
from .pipelinestep import PipelineStep
from datasource import LithopsDataSource
from profiling import profiling_context, Job, detect_runtime_environment
from util import dict_to_parset
from profiling import time_it
import logging
import os
import lithops
import pickle
import subprocess as sp
import time

logger = logging.getLogger(__name__)


class ImagingStep(PipelineStep):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters: Dict,
        output: Optional[Dict[str, S3Path]] = None,
    ):
        super().__init__(input_data_path, parameters, output)

    @property
    def input_data_path(self) -> List[S3Path]:
        return self._input_data_path

    @property
    def parameters(self) -> Dict:
        return self._parameters

    @property
    def output(self) -> S3Path:
        return self._output

    def execute_step(self, ms: List[S3Path], parameters: str, output_ms: S3Path):
        working_dir = PosixPath(os.getenv("HOME"))
        time_records = []
        data_source = LithopsDataSource()
        # Profile the download_directory method
        for partition in ms:
            partition_path = time_it(
                "download_ms", data_source.download_directory, time_records, partition
            )
            partition_path = time_it(
                "unzip", data_source.unzip, time_records, partition_path
            )

        cal_ms = [
            d
            for d in os.listdir(partition_path)
            if os.path.isdir(os.path.join(partition_path, d))
        ]

        os.chdir(f"{partition_path}")

        cmd = [
            "wsclean",
            "-size",
            "1024",
            "1024",
            "-pol",
            "I",
            "-scale",
            "5arcmin",
            "-niter",
            "100000",
            "-gain",
            "0.1",
            "-mgain",
            "0.6",
            "-auto-mask",
            "5",
            "-local-rms",
            "-multiscale",
            "-no-update-model-required",
            "-make-psf",
            "-auto-threshold",
            "3",
            "-weight",
            "briggs",
            "0",
            "-data-column",
            "CORRECTED_DATA",
            "-nmiter",
            "0",
            "-name",
            "/tmp/Cygloop-205-210-b0-1024",
            "ms",
        ]

        print(cmd)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        print("stdout:")
        print(stdout)
        print("stderr:")
        print(stderr)

        return time_records

    def _execute_step(self, *args, **kwargs):
        ms = kwargs["kwargs"]["ms"]
        parameters = kwargs["kwargs"]["parameters"]
        output_ms = kwargs["kwargs"]["output_ms"]

        print(f"Worker executing step with {len(ms)} ms paths")

        # Call the actual execution step
        with profiling_context(os.getpid()) as profiler:
            function_timers = self.execute_step(ms, parameters, output_ms)

        profiler.function_timers = function_timers
        profiler.worker_id = "id"  # Adjust based on your requirements

        env, instance_type = detect_runtime_environment()
        print(f"Worker finished step on {env} instance {instance_type}")
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

        ms = [
            S3Path.from_bucket_key(bucket=self.input_data_path.bucket, key=partition)
            for partition in keys
        ]
        parameters = (pickle.dumps(self.parameters),)
        output_ms = self.output
        start_time = time.time()
        futures = function_executor.call_async(
            func=self._execute_step,
            data={
                "args": [],
                "kwargs": {
                    "ms": ms,
                    "parameters": parameters,
                    "output_ms": output_ms,
                },
            },
            extra_env=extra_env,
        )

        result = function_executor.get_result(futures)

        end_time = time.time()

        try:
            env = result["env"]
            instance_type = result["instance_type"]
            profiler = result["profiler"]
        except KeyError as e:
            print(f"KeyError: {e}. The expected key is not in the result dictionary.")

        profiler.worker_start_tstamp = futures.stats["worker_start_tstamp"]
        profiler.worker_end_tstamp = futures.stats["worker_end_tstamp"]

        job = Job(
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

        return job
