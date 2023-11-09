from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import Union
from s3path import S3Path
import zipfile
import os


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    @abstractmethod
    def download_file(self, read_path: S3Path, write_path: PosixPath) -> None:
        pass

    @abstractmethod
    def download_directory(self, read_path: S3Path, write_path: PosixPath) -> None:
        pass

    @abstractmethod
    def upload_file(self, read_path: PosixPath, write_path: PosixPath) -> None:
        pass

    @abstractmethod
    def upload_directory(self, read_path: PosixPath, write_path: PosixPath) -> None:
        pass

    def write_parset_dict_to_file(self, parset_dict: dict, filename: str):
        with open(filename, "w") as f:
            for key, value in parset_dict.items():
                f.write(f"{key}={value}\n")

    def zip(self, ms: PosixPath) -> PosixPath:
        print(f"Zipping directory: {ms}")
        zip_filepath = PosixPath(f"{ms}.zip")  # This is the file path for the new zip file
        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(ms):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, os.path.relpath(file_path, start=ms.parent))
        return zip_filepath

    def unzip(self, ms: PosixPath) -> PosixPath:
        zip_file = zipfile.ZipFile(ms)
        part_name = ms.name.split(".")[0]
        print(f"part_name: {part_name}")
        extract_path = PosixPath(f"{ms.parent}/{part_name}.ms")
        zip_file.extractall(extract_path)
        zip_file.close()
        return extract_path
