from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import Union
from s3path import S3Path


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
