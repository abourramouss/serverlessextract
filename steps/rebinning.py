import pickle
import subprocess as sp
from pathlib import PosixPath
from typing import Dict, List, Optional
from s3path import S3Path
from .pipelinestep import PipelineStep
from datasource import LithopsDataSource
from util import dict_to_parset
import logging
import psutil
import time
from threading import Thread
from util import Profiler
import os

logger = logging.getLogger(__name__)


class RebinningStep(PipelineStep):
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

    def build_command(self, ms: S3Path, parameters: str, calibrated_ms: S3Path):
        print(psutil.cpu_count())

        profiler = Profiler()

        # Start profiling the process using its PID
        monitoring_thread = Thread(target=profiler.start_profiling)
        monitoring_thread.start()

        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        partition_path = data_source.download_directory(ms)
        aoflag_path = data_source.download_file(params["flagrebin"]["aoflag.strategy"])
        params["flagrebin"]["aoflag.strategy"] = aoflag_path
        param_path = dict_to_parset(params["flagrebin"])
        msout = str(partition_path).split("/")[-1]

        cmd = [
            "DP3",
            str(param_path),
            f"msin={partition_path}",
            f"msout=/tmp/{msout}",
            f"aoflag.strategy={aoflag_path}",
        ]

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)

        stdout, stderr = proc.communicate()

        print(stdout)
        print(stderr)
        data_source.upload_directory(
            PosixPath(f"/tmp/{msout}"), S3Path(f"{calibrated_ms}/{msout}")
        )
        # Stop the profiler after the process completes
        profiler.stop_profiling()
        monitoring_thread.join()  # Ensure that the monitoring thread finishes

        return profiler
