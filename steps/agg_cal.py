import pickle
import subprocess as sp
from typing import Dict, List, Optional
from s3path import S3Path
from .pipelinestep import PipelineStep
from datasource import LithopsDataSource
from util import dict_to_parset
from profiling import time_it
import logging
import os
from pathlib import PosixPath

logger = logging.getLogger(__name__)
logger.propagate = True


class CalibrationSubstractionApplyCalibrationStep(PipelineStep):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters: List[Dict],
        output: Optional[Dict[str, S3Path]] = None,
    ):
        super().__init__(input_data_path, parameters, output)

    @property
    def input_data_path(self) -> List[S3Path]:
        return self._input_data_path

    @property
    def parameters(self) -> List[Dict]:
        return self._parameters

    @property
    def output(self) -> S3Path:
        return self._output

    def execute_step(self, ms: S3Path, parameters: List[str], output_ms: S3Path):
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()

        calibration_params, substraction_params, apply_params = [
            pickle.loads(param) for param in parameters
        ]
        time_records = []

        # Calibration Step
        output_h5 = f"{working_dir}/output.h5"
        logger.info("Starting calibration step")
        cal_partition_path = time_it(
            "download_ms",
            data_source.download_file,
            time_records,
            ms,
            working_dir,
        )

        logger.info("Downloaded measurement set to:", cal_partition_path)

        cal_partition_path = time_it(
            "unzip", data_source.unzip, time_records, cal_partition_path
        )
        logger.info("Unzipped partition path:", cal_partition_path)
        logger.info("Directory contents:", os.listdir(str(cal_partition_path)))

        sourcedb_dir = time_it(
            "download_parameters",
            data_source.download_directory,
            time_records,
            calibration_params["cal"]["cal.sourcedb"],
        )
        calibration_params["cal"]["cal.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(calibration_params["cal"])
        logger.info("Output H5 file will be:", output_h5)

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"cal.h5parm={output_h5}",
            f"cal.sourcedb={sourcedb_dir}",
        ]
        logger.info("Executing DP3 command for calibration")

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = time_it("execute_script", proc.communicate, time_records)

        if proc.returncode != 0:
            logger.info("[ERROR] DP3 calibration script execution failed")
        else:
            logger.info("DP3 calibration script executed successfully")

        logger.info("Listing temporary directory contents")
        logger.info(os.listdir("/tmp"))
        output_h5_path = PosixPath(output_h5)

        # Substraction Step
        logger.info("Starting substraction step")
        logger.info(
            "[INFO] Calibrated partition and output H5 path:",
            cal_partition_path,
            output_h5_path,
        )

        sourcedb_dir = time_it(
            "download_parameters_1",
            data_source.download_directory,
            time_records,
            substraction_params["sub"]["sub.sourcedb"],
        )
        substraction_params["sub"]["sub.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(substraction_params["sub"])

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"sub.applycal.parmdb={output_h5}",
            f"sub.sourcedb={sourcedb_dir}",
        ]
        logger.info(" Executing DP3 command for substraction")

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = time_it("execute_script", proc.communicate, time_records)
        if proc.returncode != 0:
            logger.info("DP3 substraction script execution failed")
        else:
            logger.info("DP3 substraction script executed successfully")

        # ApplyCalibration Step
        logger.info("Starting ApplyCalibration step")
        params = apply_params
        param_path = dict_to_parset(params["apply"])
        sub_combined_path = cal_partition_path

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"apply.parmdb={output_h5}",
        ]
        logger.info("Executing DP3 command for ApplyCalibration")

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = time_it("execute_command", proc.communicate, time_records)
        logger.info(stdout)
        if proc.returncode != 0:
            logger.info("DP3 ApplyCalibration script execution failed")
        else:
            logger.info("DP3 ApplyCalibration script executed successfully")

        # Zipping the processed directory
        zipped_imaging = time_it(
            "zip", data_source.zip_without_compression, time_records, sub_combined_path
        )

        partition_name = str(zipped_imaging).split("/")[-1]
        time_it(
            "upload_zip",
            data_source.upload_file,
            time_records,
            zipped_imaging,
            S3Path(f"{output_ms}/{partition_name}"),
        )

        return time_records
