# Local baseline of the pipeline, without simulating the cloud enviroment, single MS.
from abc import ABC, abstractmethod
import os
from typing import List, Union
import lithops
from typing import Dict, Optional
from lithops import Storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3path import S3Path
import pickle
from pathlib import PosixPath
import subprocess as sp


def dict_to_parset(
    data, output_dir=PosixPath("/tmp"), filename="output.parset"
) -> PosixPath:
    lines = []

    for key, value in data.items():
        # Check if the value is another dictionary
        if isinstance(value, dict):
            lines.append(f"[{key}]")
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):  # Check for nested dictionaries
                    lines.append(f"[{sub_key}]")
                    for sub_sub_key, sub_sub_value in sub_value.items():
                        lines.append(f"{sub_sub_key} = {sub_sub_value}")
                else:
                    lines.append(f"{sub_key} = {sub_value}")
        else:
            lines.append(f"{key} = {value}")

    # Convert the list of lines to a single string
    parset_content = "\n".join(lines)

    # Ensure the directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / filename
    with output_path.open("w") as file:
        file.write(parset_content)

    return output_path


def s3_to_local_path(
    s3_path: S3Path, base_local_dir: PosixPath = PosixPath("/tmp")
) -> PosixPath:
    """Converts an S3Path to a local file path."""
    local_path = os.path.join(base_local_dir, s3_path.bucket, s3_path.key)
    return PosixPath(local_path)


def local_to_s3_path(local_path: str, base_local_dir: str = "/tmp") -> S3Path:
    """Converts a local file path to an S3Path."""
    local_path = os.path.abspath(local_path)
    components = local_path.replace(base_local_dir, "").split(os.path.sep)[1:]
    bucket = components[0]
    key = "/".join(components[1:])
    return S3Path(f"{bucket}/{key}")


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    @abstractmethod
    def download_file(
        self, read_path: Union[S3Path, PosixPath], write_path: PosixPath
    ) -> None:
        pass

    @abstractmethod
    def download_directory(
        self, read_path: Union[S3Path, PosixPath], write_path: PosixPath
    ) -> None:
        pass

    @abstractmethod
    def upload_file(
        self, read_path: PosixPath, write_path: Union[S3Path, PosixPath]
    ) -> None:
        pass

    @abstractmethod
    def upload_directory(
        self, read_path: PosixPath, write_path: Union[S3Path, PosixPath]
    ) -> None:
        pass

    def write_parset_dict_to_file(self, parset_dict: dict, filename: str):
        with open(filename, "w") as f:
            for key, value in parset_dict.items():
                f.write(f"{key}={value}\n")


class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()

    def download_file(
        self, read_path: S3Path, base_path: PosixPath = PosixPath("/tmp")
    ) -> PosixPath:
        """Download a file from S3 and returns the local path."""
        if isinstance(read_path, S3Path):
            try:
                local_path = s3_to_local_path(read_path, base_local_dir=str(base_path))
                os.makedirs(local_path.parent, exist_ok=True)
                self.storage.download_file(
                    read_path.bucket, read_path.key, str(local_path)
                )
                return PosixPath(local_path)
            except Exception as e:
                print(f"Failed to download file {read_path.key}: {e}")

    def download_directory(
        self, read_path: S3Path, base_path: PosixPath = PosixPath("/tmp")
    ) -> PosixPath:
        """Download a directory from S3 and returns the local path."""
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(base_path)
        )

        for key in keys:
            s3_file_path = S3Path.from_bucket_key(read_path.bucket, key)
            self.download_file(
                s3_file_path, base_path=base_path
            )  # Using the same base_path for file downloads

        return PosixPath(local_directory_path)

    def upload_file(self, read_path: PosixPath, write_path: S3Path) -> None:
        """Uploads a local file to S3."""
        try:
            self.storage.upload_file(str(read_path), write_path.bucket, write_path.key)
        except Exception as e:  # Consider narrowing down the exceptions caught.
            print(f"Failed to upload file {read_path} to {write_path}. Error: {e}")

    def upload_directory(self, read_path: PosixPath, write_base_path: S3Path) -> None:
        """Uploads a local directory to S3, maintaining its structure."""
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []

            for dirpath, _, filenames in os.walk(read_path):
                for filename in filenames:
                    local_file_path = PosixPath(dirpath).joinpath(filename)
                    relative_path = local_file_path.relative_to(read_path)
                    s3_path = write_base_path.joinpath(relative_path)

                    futures.append(
                        executor.submit(self.upload_file, local_file_path, s3_path)
                    )

            for future in as_completed(futures):
                future.result()


class PipelineStep(ABC):
    def __init__(
        self,
        input_data_path: Dict[str, S3Path],
        parameters: Dict,
        output: Optional[Dict[str, S3Path]] = None,
    ):
        self._input_data_path = input_data_path
        self._parameters = parameters
        self._output = output

    @property
    @abstractmethod
    def input_data_path(self) -> List[S3Path]:
        pass

    @property
    @abstractmethod
    def parameters(self) -> Dict:
        pass

    @property
    @abstractmethod
    def output(self) -> S3Path:
        pass

    @abstractmethod
    def build_command(self, *args, **kwargs):
        pass

    def run(self):
        extra_env = {"HOME": "/tmp"}
        function_executor = lithops.FunctionExecutor()

        keys = lithops.Storage().list_keys(
            bucket=self.input_data_path.bucket,
            prefix=self.input_data_path.key,
        )

        # Create an empty set to hold unique directories
        unique_partitions = set()

        # Iterate over each key
        for key in keys:
            # Split the key into its parts
            parts = key.split("/")

            # Extract the directory that ends in .ms
            partition = next((part for part in parts if part.endswith(".ms")), None)
            if partition:
                # Combine the prefix with the partition
                full_partition_path = "/".join(parts[: parts.index(partition) + 1])
                unique_partitions.add(full_partition_path)

        s3_paths = {
            (
                S3Path.from_bucket_key(
                    bucket=self.input_data_path.bucket, key=partition
                ),
                pickle.dumps(self.parameters),
                self.output,
            )
            for partition in unique_partitions
        }

        futures = function_executor.map(
            self.build_command,
            s3_paths,
            extra_env=extra_env,
        )
        results = function_executor.get_result(futures)
        return results


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

    def build_command(
        self, ms: S3Path, parameters: str, calibrated_ms: S3Path
    ) -> List[str]:
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

        print("Command:", cmd)

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        print("stdout:", stdout)
        print("stderr:", stderr)

        calibrated_ms_path = data_source.upload_directory(
            PosixPath(f"/tmp/{msout}"), S3Path(f"{calibrated_ms}/{msout}")
        )

        print(calibrated_ms_path)
        return calibrated_ms_path


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

    def build_command(
        self, calibrated_ms: S3Path, parameters: str, h5: S3Path
    ) -> List[str]:
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        cal_partition_path = data_source.download_directory(calibrated_ms)
        sourcedb_dir = data_source.download_directory(params["cal"]["cal.sourcedb"])
        params["cal"]["cal.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(params["cal"])

        output_ms = str(cal_partition_path).split("/")[-1]
        output_h5 = output_ms.replace(".ms", ".h5")

        print("Calibrated MS:", cal_partition_path)
        print("Param path:", param_path)
        print("Output H5:", output_h5)
        print("SourceDB:", sourcedb_dir)

        cmd = [
            "DP3",
            str(param_path),
            f"msin={cal_partition_path}",
            f"cal.h5parm=/tmp/{output_h5}",
            f"cal.sourcedb={sourcedb_dir}",
        ]

        print("Command:", cmd)
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        stdout, stderr = proc.communicate()

        data_source.upload_file(
            f"/tmp/{output_h5}",
            S3Path(f"{h5}/h5/{output_h5}"),
        )

        data_source.upload_directory(cal_partition_path, S3Path(f"{h5}/ms/{output_ms}"))

        return S3Path(f"{h5}/h5/{output_h5}"), S3Path(f"{h5}/ms/{output_ms}")


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
    ) -> List[str]:
        data_source = LithopsDataSource()
        params = pickle.loads(parameters)
        cal_partition_path = data_source.download_directory(calibrated_ms)
        sourcedb_dir = data_source.download_directory(params["sub"]["sub.sourcedb"])
        params["sub"]["sub.sourcedb"] = sourcedb_dir
        param_path = dict_to_parset(params["sub"])
        h5_path = str(calibrated_ms).replace("ms", "h5")
        output_h5 = data_source.download_file(S3Path(h5_path))
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
        stdout, stderr = proc.communicate()
        print(stdout, stderr)

        data_source.upload_directory(
            cal_partition_path, S3Path(f"{substracted_ms}/ms/{output_ms}")
        )


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
    ) -> List[str]:
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


if __name__ == "__main__":
    parameters = {
        "RebinningStep": {
            "input_data_path": S3Path("/extract/partitions/partitio1/"),
            "parameters": {
                "flagrebin": {
                    "steps": "[aoflag, avg, count]",
                    "aoflag.type": "aoflagger",
                    "aoflag.memoryperc": 90,
                    "aoflag.strategy": S3Path(
                        "/extract/parameters/rebinning/STEP1-NenuFAR64C1S.lua"
                    ),
                    "avg.type": "averager",
                    "avg.freqstep": 4,
                    "avg.timestep": 8,
                }
            },
            "output": S3Path("/extract/extract-data/rebinning_out/"),
        },
        "CalibrationStep": {
            "input_data_path": S3Path("/extract/extract-data/rebinning_out/"),
            "parameters": {
                "cal": {
                    "msin": "",
                    "msin.datacolumn": "DATA",
                    "msout": ".",
                    "steps": "[cal]",
                    "cal.type": "ddecal",
                    "cal.mode": "diagonal",
                    "cal.sourcedb": S3Path(
                        "/extract/parameters/calibration/STEP2A-apparent.sourcedb"
                    ),
                    "cal.h5parm": "",
                    "cal.solint": 4,
                    "cal.nchan": 4,
                    "cal.maxiter": 50,
                    "cal.uvlambdamin": 5,
                    "cal.smoothnessconstraint": 2e6,
                }
            },
            "output": S3Path("/extract/extract-data/calibration_out/"),
        },
        "SubstractionStep": {
            "input_data_path": S3Path("/extract/extract-data/calibration_out/"),
            "parameters": {
                "sub": {
                    "msin": "",
                    "msin.datacolumn": "DATA",
                    "msout": ".",
                    "msout.datacolumn": "SUBTRACTED_DATA",
                    "steps": "[sub]",
                    "sub.type": "h5parmpredict",
                    "sub.sourcedb": S3Path(
                        "/extract/parameters/calibration/STEP2A-apparent.sourcedb"
                    ),
                    "sub.directions": "[[CygA],[CasA]]",
                    "sub.operation": "subtract",
                    "sub.applycal.parmdb": "",
                    "sub.applycal.steps": "[sub_apply_amp,sub_apply_phase]",
                    "sub.applycal.correction": "fulljones",
                    "sub.applycal.sub_apply_amp.correction": "amplitude000",
                    "sub.applycal.sub_apply_phase.correction": "phase000",
                }
            },
            "output": S3Path("/extract/extract-data/substraction_out/"),
        },
        "ApplyCalibrationStep": {
            "input_data_path": S3Path("/extract/extract-data/substraction_out/"),
            "parameters": {
                "apply": {
                    "msin": "",
                    "msin.datacolumn": "SUBTRACTED_DATA",
                    "msout": ".",
                    "msout.datacolumn": "CORRECTED_DATA",
                    "steps": "[apply]",
                    "apply.type": "applycal",
                    "apply.steps": "[apply_amp,apply_phase]",
                    "apply.apply_amp.correction": "amplitude000",
                    "apply.apply_phase.correction": "phase000",
                    "apply.direction": "[Main]",
                    "apply.parmdb": "",
                }
            },
            "output": S3Path("/extract/extract-data/applycal_out/"),
        },
    }

    RebinningStep(
        input_data_path=parameters["RebinningStep"]["input_data_path"],
        parameters=parameters["RebinningStep"]["parameters"],
        output=parameters["RebinningStep"]["output"],
    ).run()
    """
    CalibrationStep(
        input_data_path=parameters["CalibrationStep"]["input_data_path"],
        parameters=parameters["CalibrationStep"]["parameters"],
        output=parameters["CalibrationStep"]["output"],
    ).run()

    SubstractionStep(
        input_data_path=parameters["SubstractionStep"]["input_data_path"],
        parameters=parameters["SubstractionStep"]["parameters"],
        output=parameters["SubstractionStep"]["output"],
    ).run()

    ApplyCalibrationStep(
        input_data_path=parameters["ApplyCalibrationStep"]["input_data_path"],
        parameters=parameters["ApplyCalibrationStep"]["parameters"],
        output=parameters["ApplyCalibrationStep"]["output"],
    ).run()

    """
