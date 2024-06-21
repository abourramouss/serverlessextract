from abc import ABC, abstractmethod
from pathlib import PosixPath, Path
from s3path import S3Path
import zipfile
import os
import logging
from radiointerferometry.profiling import time_it

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class S3Path:
    def __init__(self, bucket: str, key: str, file_ext: str = None):
        self._bucket = bucket
        self._key = key
        self._file_ext = file_ext

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

    def __repr__(self):
        return f"/{self._bucket}/{self._key}{('.' + self._file_ext if self._file_ext else '')}"

    def __str__(self):
        return self.__repr__()


class InputS3(S3Path):
    def __init__(
        self, bucket: str, key: str, file_ext: str = None, dynamic: bool = False
    ):
        super().__init__(bucket, key, file_ext)
        self.dynamic = dynamic


class OutputS3(S3Path):
    def __init__(
        self,
        bucket: str,
        key: str,
        file_ext: str = None,
        file_name: str = None,
        remote_key_ow: str = None,
    ):
        super().__init__(bucket, key, file_ext)
        self._file_name = file_name
        self.remote_ow = remote_key_ow


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    def __init__(self):
        self.timings = []

    @abstractmethod
    def exists(self, path: S3Path) -> bool:
        pass

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

    def zip_without_compression(self, ms: Path) -> Path:
        logging.info(f"Starting zipping process for: {ms}")
        zip_filepath = ms.with_name(ms.name + ".zip")

        if zip_filepath.exists() and zip_filepath.is_dir():
            logging.error(
                f"Cannot create a zip file as a directory with the name {zip_filepath} exists."
            )
            raise IsADirectoryError(
                f"Cannot create a zip file as a directory with the name {zip_filepath} exists."
            )

        if ms.is_dir():
            with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_STORED) as zipf:
                for root, dirs, files in os.walk(ms):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = ms.name / file_path.relative_to(ms)
                        zipf.write(file_path, arcname)
        else:
            logging.error(f"{ms} is not a directory.")
            raise NotADirectoryError(f"Expected a directory, got {ms}")

        logging.info(f"Created zip file at {zip_filepath}")
        return zip_filepath

    def unzip(self, ms: Path) -> Path:
        logging.info(f"Extracting zip file at {ms}")
        if ms.suffix != ".zip":
            logging.error(f"Expected a .zip file, got {ms}")
            raise ValueError(f"Expected a .zip file, got {ms}")

        extract_path = ms.parent
        logging.info(f"Extracting to directory: {extract_path}")

        with zipfile.ZipFile(ms, "r") as zipf:
            zip_contents = zipf.namelist()
            logging.debug(f"Zip contents: {zip_contents}")

            zipf.extractall(extract_path)

            extracted_dir = extract_path / ms.stem

        ms.unlink()
        logging.info(f"Deleted zip file at {ms}")

        logging.info(f"Extracted to directory: {extracted_dir}")
        return extracted_dir
