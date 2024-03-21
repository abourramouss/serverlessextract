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
logger.propagate = True


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

    def execute_step(self, ms: S3Path, parameters: str, output_ms: S3Path):
        working_dir = PosixPath(
            os.getenv("HOME")
        )  # this is set to /tmp, to respect lambda convention.
        time_records = []
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)

        # Profile the download_directory method
        partition_path = time_it(
            "download_ms", data_source.download_directory, time_records, ms
        )
        partition_path = time_it(
            "unzip", data_source.unzip, time_records, partition_path
        )

        logger.info("Listing directory")
        ms_name = str(partition_path).split("/")[-1]
        logger.debug(partition_path)
        logger.debug(ms_name)

        # Profile the download_file method
        aoflag_path = time_it(
            "download_parameters",
            data_source.download_file,
            time_records,
            params["flagrebin"]["aoflag.strategy"],
        )
        logger.debug(f"Ao flag path: {aoflag_path}")
        params["flagrebin"]["aoflag.strategy"] = aoflag_path
        param_path = dict_to_parset(params["flagrebin"])

        msout = f"{working_dir}/{ms_name}"

        logger.debug(os.listdir(partition_path))
        cmd = [
            "DP3",
            str(param_path),
            f"msin={partition_path}",
            f"msout={msout}",
            f"aoflag.strategy={aoflag_path}",
        ]

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)

        # Profile the process execution
        stdout, stderr = time_it("execute_script", proc.communicate, time_records)

        logger.debug("Stdout:")
        logger.debug(stdout)
        logger.debug("Stderr:")
        logger.debug(stderr)

        posix_source = time_it(
            "zip", data_source.zip_without_compression, time_records, PosixPath(msout)
        )

        # Profile the upload_directory method
        time_it(
            "upload_rebinnedms",
            data_source.upload_file,
            time_records,
            posix_source,
            S3Path(f"{output_ms}/{ms_name}.zip"),
        )

        shutil.rmtree(partition_path)
        shutil.rmtree(msout)
        os.remove(posix_source)

        return time_records
