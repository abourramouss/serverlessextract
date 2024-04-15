from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import Union
from s3path import S3Path
import zipfile
import os
import shutil


class InputS3:
    def __init__(self, bucket: str, key: str):
        self._bucket = bucket
        self._key = key

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = value

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    def __str__(self):
        return f"/{self._bucket}/{self._key}"


class OutputS3:
    def __init__(self, bucket: str, key: str, file_ext=None, separate_dir=False):
        self._bucket = bucket
        self._key = key
        self._file_ext = file_ext
        self._separate_dir = separate_dir

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = value

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def file_ext(self):
        return self._file_ext

    @property
    def separate_dir(self):
        return self._separate_dir

    def __str__(self):
        return f"/{self._bucket}/{self._key}"


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

    def zip_without_compression(self, ms: PosixPath) -> PosixPath:
        print(f"Zipping directory: {ms}")
        zip_filepath = ms.with_suffix(".zip")

        if zip_filepath.exists() and zip_filepath.is_dir():
            raise IsADirectoryError(
                f"Cannot create a zip file as a directory with the name {zip_filepath} exists."
            )

        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_STORED) as zipf:
            for root, dirs, files in os.walk(ms):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=ms.parent)
                    zipf.write(file_path, arcname=arcname)

        print(f"Created zip file at {zip_filepath}")
        return zip_filepath

    def unzip(self, ms: PosixPath) -> PosixPath:
        print(f"Extracting zip file at {ms}")
        if ms.suffix != ".zip":
            raise ValueError(f"Expected a .zip file, got {ms}")

        extract_path = ms.parent / ms.stem
        if not extract_path.exists():
            extract_path.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(ms, "r") as zipf:
            zipf.extractall(extract_path)

        print(f"Extracted to: {extract_path}")
        return extract_path
