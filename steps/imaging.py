from pathlib import PosixPath
from typing import Dict, List, Optional
from s3path import S3Path
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
logger.propagate = True


class ImagingStep:
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

    def execute_step(self, ms: List[S3Path], parameters: str, output_ms: S3Path, cpus):
        working_dir = PosixPath(os.getenv("HOME"))
        time_records = []
        data_source = LithopsDataSource()

        # Initialize an empty list to store partition paths
        partitions = []

        for partition in ms:
            partition_path = time_it(
                "download_ms", data_source.download_directory, time_records, partition
            )
            partition_path = time_it(
                "unzip", data_source.unzip, time_records, partition_path
            )

            partitions.append(str(partition_path))

        cmd = [
            "wsclean",
            "-j",
            str(cpus),
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
        ]

        # Append the paths of all partitions to the command
        cmd.extend(partitions)

        logger.info(cmd)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)

        stdout, stderr = proc.communicate()

        logger.info("Listing directory")
        logger.info(os.listdir(f"{working_dir}"))
        image_fits_file = next(
            (f for f in os.listdir(working_dir) if f.endswith("-image.fits")), None
        )
        if image_fits_file:
            posix_source = PosixPath(working_dir) / image_fits_file
            logger.info(f"Uploading {image_fits_file} to S3: {output_ms}")
            time_it(
                "upload_image_fits",
                data_source.upload_file,
                time_records,
                posix_source,
                S3Path(f"{output_ms}/{image_fits_file}"),
            )
        else:
            logger.error("No -image.fits file found to upload.")
        print("stdout:")
        print(stdout)
        print("stderr:")
        print(stderr)

        return time_records

    def _execute_step(self, id, *args, **kwargs):
        ms = kwargs["kwargs"]["ms"]
        parameters = kwargs["kwargs"]["parameters"]
        output_ms = kwargs["kwargs"]["output_ms"]
        cpus = kwargs["kwargs"]["cpus"]

        print(f"Worker executing step with {len(ms)} ms paths")

        # Call the actual execution step
        with profiling_context(os.getpid()) as profiler:
            function_timers = self.execute_step(ms, parameters, output_ms, cpus)

        profiler.function_timers = function_timers
        profiler.worker_id = id

        env, instance_type = detect_runtime_environment()
        print(f"Worker finished step on {env} instance {instance_type}")
        return {"profiler": profiler, "env": env, "instance_type": instance_type}

    def run(
        self,
        func_limit: Optional[int] = None,
    ):
        # Parameters to optimize
        runtime_memory = 2000
        cpus_per_worker = 4
        extra_env = {"HOME": "/tmp", "OPENBLAS_NUM_THREADS": "1"}
        function_executor = lithops.FunctionExecutor(
            runtime_memory=runtime_memory,
            runtime_cpu=cpus_per_worker,
        )
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

        chunk_size = f"{lithops.Storage().head_object(self.input_data_path.bucket, keys[0])['content-length']}/{1024**2}"

        parameters = (pickle.dumps(self.parameters),)
        output_ms = self.output
        start_time = time.time()
        future = function_executor.call_async(
            func=self._execute_step,
            data={
                "args": [],
                "kwargs": {
                    "ms": ms,
                    "parameters": parameters,
                    "output_ms": output_ms,
                    "cpus": cpus_per_worker,
                },
            },
            extra_env=extra_env,
        )

        logger.info(f"parameters: {self.parameters}")

        result = function_executor.get_result([future])

        end_time = time.time()

        try:
            env = result["env"]
            instance_type = result["instance_type"]
            profiler = result["profiler"]
        except KeyError as e:
            print(f"KeyError: {e}. The expected key is not in the result dictionary.")

        profiler.worker_start_tstamp = future.stats["worker_start_tstamp"]
        profiler.worker_end_tstamp = future.stats["worker_end_tstamp"]

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
