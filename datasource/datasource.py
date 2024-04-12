from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import Union
from s3path import S3Path
import zipfile
import os
import subprocess


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
    def __init__(self, bucket: str, key: str, naming_pattern="{id}.output"):
        self._bucket = bucket
        self._key = key
        self._naming_pattern = naming_pattern

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
    def naming_pattern(self):
        return self._naming_pattern

    @naming_pattern.setter
    def naming_pattern(self, value):
        self._naming_pattern = value

    def formatted_filename(self, **kwargs):
        return self._naming_pattern.format(**kwargs)

    def construct_s3_path(self, id=None):
        if id is not None:
            filename = self.naming_pattern.format(id=id)
        else:
            filename = self.naming_pattern.format(id="default")  # default or some logic
        return f"{self._bucket}/{os.path.join(self._key, filename)}"

    def __str__(self):
        return f"s3://{self._bucket}/{self._key}/{self.naming_pattern}"


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
        zip_filepath = PosixPath(f"{ms.parent}/{ms.name}.zip")
        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_STORED) as zip_file:
            for root, dirs, files in os.walk(ms):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=ms.parent)
                    zip_file.write(file_path, arcname)
        return zip_filepath

    def unzip(self, ms: PosixPath) -> PosixPath:
        with zipfile.ZipFile(ms, "r") as zip_file:
            extract_path = ms.parent
            zip_file.extractall(extract_path)

            # Collect the first level of directories/files in the zip
            root_items = {item.split("/")[0] for item in zip_file.namelist()}

        # Determine if all contents are under a single root directory
        if len(root_items) == 1:
            part_name = next(iter(root_items))
            new_ms_path = extract_path / part_name
            if not (new_ms_path.exists() and new_ms_path.is_dir()):
                os.makedirs(new_ms_path, exist_ok=True)
        else:
            new_ms_path = extract_path

        print(f"Unzipped to: {new_ms_path}")
        return new_ms_path
