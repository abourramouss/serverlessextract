from abc import ABC, abstractmethod
from pathlib import PosixPath
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
    ):
        super().__init__(bucket, key, file_ext)
        self._file_name = file_name


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

    def zip_without_compression(self, ms: PosixPath) -> PosixPath:
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
                        file_path = PosixPath(root) / file
                        arcname = file_path.relative_to(ms)
                        zipf.write(file_path, arcname)
        elif ms.is_file():
            with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_STORED) as zipf:
                arcname = ms.name
                zipf.write(ms, arcname)
        else:
            logging.error(f"{ms} is neither a file nor a directory.")
            raise FileNotFoundError(f"No such file or directory: {ms}")

        logging.info(f"Created zip file at {zip_filepath}")
        return zip_filepath

    def unzip(self, ms: PosixPath) -> PosixPath:
        logging.info(f"Extracting zip file at {ms}")
        if ms.suffix != ".zip":
            logging.error(f"Expected a .zip file, got {ms}")
            raise ValueError(f"Expected a .zip file, got {ms}")

        extract_path = ms.parent / ms.stem
        if not extract_path.exists():
            extract_path.mkdir(parents=True, exist_ok=True)
        else:
            logging.warning(f"Already exists")
            return extract_path

        with zipfile.ZipFile(ms, "r") as zipf:
            zip_contents = zipf.namelist()
            logging.debug(f"Zip contents: {zip_contents}")
            if (
                len(zip_contents) == 1
                and PosixPath(zip_contents[0]).name == zip_contents[0]
            ):
                single_file_path = extract_path / zip_contents[0]
                zipf.extract(zip_contents[0], extract_path)
                logging.info(f"Extracted single file to: {single_file_path}")
                return single_file_path
            else:
                zipf.extractall(extract_path)
                logging.info(f"Extracted to directory: {extract_path}")

        return extract_path
