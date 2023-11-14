import pickle
import subprocess as sp
from typing import Dict, List, Optional
from s3path import S3Path
from .pipelinestep import PipelineStep
from datasource import LithopsDataSource
from util import dict_to_parset, time_it
import logging
import os
from pathlib import PosixPath

logger = logging.getLogger(__name__)


class CalibrationStep(PipelineStep):
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

    def build_command(self, calibrated_ms: S3Path, parameters: str, h5: S3Path):
        working_dir = PosixPath(os.getenv("HOME"))
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        time_records = []

        cal_partition_path = time_it(
            "download_ms", data_source.download_directory, time_records, calibrated_ms
        )
        cal_partition_path = time_it(
            "unzip", data_source.unzip, time_records, cal_partition_path
        )

        sourcedb_dir = time_it(
            "download_parameters",
            data_source.download_directory,
            time_records,
            params["cal"]["cal.sourcedb"],
        )
        params["cal"]["cal.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(params["cal"])

        print("cal partition path:", cal_partition_path)
        output_h5 = cal_partition_path.replace("ms", "h5")
        print("Output H5:", output_h5)

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"cal.h5parm={working_dir}/{output_h5}",
            f"cal.sourcedb={sourcedb_dir}",
        ]

        print("Command:", cmd)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)

        stdout, stderr = time_it("execute_script", proc.communicate, time_records)

        # Zip the two files together and send them to s3 (h5 and ms)

        # Upload h5 file
        time_it(
            "upload_h5",
            data_source.upload_file,
            time_records,
            f"/tmp/{output_h5}",
            S3Path(f"{h5}/h5/{output_h5}"),
        )

        # Zip ms directory
        cal_zip = time_it("zip", data_source.zip, time_records, cal_partition_path)

        # Upload ms zipped
        time_it(
            "upload_calibratedms",
            data_source.upload_file,
            time_records,
            cal_zip,
            S3Path(f"{h5}/ms/{output_ms}.zip"),
        )
        """return (
            time_records,
            S3Path(f"{h5}/h5/{output_h5}"),
            S3Path(f"{h5}/ms/{output_ms}"),
        )

        """
        return time_records


class SubstractionStep(PipelineStep):
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

    def build_command(
        self, calibrated_ms: S3Path, parameters: str, substracted_ms: S3Path
    ):
        time_records = []
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)

        print("Calibrated MS:", calibrated_ms)
        cal_partition_path = time_it(
            "download_ms", data_source.download_directory, time_records, calibrated_ms
        )
        cal_partition_path = time_it(
            "unzip", data_source.unzip, time_records, cal_partition_path
        )

        sourcedb_dir = time_it(
            "download_parameters_1",
            data_source.download_directory,
            time_records,
            params["sub"]["sub.sourcedb"],
        )
        params["sub"]["sub.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(params["sub"])
        h5_path = str(calibrated_ms).replace("ms", "h5")
        output_h5 = time_it(
            "time_records_2", data_source.download_file, time_records, S3Path(h5_path)
        )
        output_ms = str(calibrated_ms).split("/")[-1]

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"sub.applycal.parmdb={output_h5}",
            f"sub.sourcedb={sourcedb_dir}",
        ]
        print(cmd)

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = time_it("execute_script", time_records, proc.communicate)

        time_it(
            "upload_directory",
            data_source.upload_directory,
            time_records,
            cal_partition_path,
            S3Path(f"{substracted_ms}/ms/{output_ms}"),
        )

        return time_records


class ApplyCalibrationStep(PipelineStep):
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

    def build_command(
        self, calibrated_ms: S3Path, parameters: str, substracted_ms: S3Path
    ):
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        cal_partition_path = data_source.download_directory(calibrated_ms)
        param_path = dict_to_parset(params["apply"])
        h5_path = str(calibrated_ms).replace("ms", "h5")
        h5_path = h5_path.replace("substraction_out", "calibration_out")
        print("H5 path:", h5_path)
        input_h5 = data_source.download_file(S3Path(h5_path))
        output_ms = str(calibrated_ms).split("/")[-1]

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"apply.parmdb={input_h5}",
        ]

        print("command", cmd)

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()
        print(stdout, stderr)

        data_source.upload_directory(
            cal_partition_path, S3Path(f"{substracted_ms}/ms/{output_ms}")
        )
