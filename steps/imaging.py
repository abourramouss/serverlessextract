import pickle
import subprocess as sp
from pathlib import PosixPath
from typing import Dict, List, Optional
from s3path import S3Path
from .pipelinestep import PipelineStep
from datasource import LithopsDataSource
from util import dict_to_parset
from profiling import time_it
import logging
import os
import shutil

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

    def execute_step(self, ms: S3Path, parameters: str, output_ms: S3Path):
        working_dir = PosixPath(
            os.getenv("HOME")
        )
        time_records = []

        data_source = LithopsDataSource()
        # Profile the download_directory method
        partition_path = time_it(
            "download_ms", data_source.download_directory, time_records, ms
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
            'ms'
        ]

        
        print(cmd)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        print("stdout:")
        print(stdout)
        print("stderr:")
        print(stderr)

        


        
