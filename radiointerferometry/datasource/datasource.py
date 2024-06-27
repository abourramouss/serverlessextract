from abc import ABC, abstractmethod
from pathlib import Path, Path
from s3path import S3Path
import zipfile
import os
import logging
from radiointerferometry.profiling import time_it

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class S3PathBase:
    def __init__(
        self, bucket: str, key: str, file_ext: str = None, base_local_path: str = "/tmp"
    ):
        self._bucket = bucket
        self._key = key
        self._file_ext = file_ext
        self._base_local_path = base_local_path if base_local_path else "/tmp"
        self.local_path = None

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
    def base_local_path(self):
        return self._base_local_path

    @base_local_path.setter
    def base_local_path(self, value):
        self._base_local_path = value if value else "/tmp"

    def __repr__(self):
        return f"{self._base_local_path}/{self._bucket}/{self._key}{('.' + self._file_ext if self._file_ext else '')}"

    def __str__(self):
        return self.__repr__()

    def to_local_path(self, remote_key_ow=None):
        return LocalPath(
            self._base_local_path,
            self._bucket,
            self._key,
            self._file_ext,
            remote_key_ow,
        )


class InputS3(S3PathBase):
    def __init__(
        self,
        bucket: str,
        key: str,
        file_ext: str = None,
        dynamic: bool = False,
        base_local_path: str = "/tmp",
    ):
        super().__init__(bucket, key, file_ext, base_local_path)
        self.dynamic = dynamic
        print(
            f"Initialized InputS3 with bucket: {bucket}, key: {key}, file_ext: {file_ext}, "
            f"dynamic: {dynamic}, base_local_path: {self._base_local_path}"
        )


class LocalPath:
    def __init__(self, base_local_path, bucket, key, file_ext=None, remote_key_ow=None):
        print(
            f"Initializing LocalPath with {base_local_path}, {bucket}, {key}, {file_ext}, {remote_key_ow}"
        )
        self.base_local_path = base_local_path
        self.bucket = bucket
        self.key = key
        self.file_ext = file_ext
        self.remote_key_ow = remote_key_ow
        self.path = Path(
            f"{self.base_local_path}/{self.bucket}/{self.key}{('.' + self.file_ext if self.file_ext else '')}"
        )

    def __str__(self):
        return str(self.path)

    def __fspath__(self):
        return str(self.path)

    def __getattr__(self, name):
        return getattr(self.path, name)

    @property
    def parent(self):
        return self.path.parent

    def get_remote_path(self):
        return OutputS3(
            bucket=self.bucket,
            key=self.key,
            file_ext=self.file_ext,
            file_name=self.path.name,
            remote_key_ow=self.remote_key_ow,
            base_local_path=self.base_local_path,
        )


class OutputS3(S3PathBase):
    def __init__(
        self,
        bucket: str,
        key: str,
        file_ext: str = None,
        file_name: str = None,
        remote_key_ow: str = None,
        base_local_path: str = "/tmp",
    ):
        super().__init__(bucket, key, file_ext, base_local_path)
        self._file_name = file_name
        self.remote_ow = remote_key_ow
        print(
            f"Initialized OutputS3 with bucket: {bucket}, key: {key}, file_ext: {file_ext}, "
            f"file_name: {file_name}, remote_key_ow: {remote_key_ow}, base_local_path: {self._base_local_path}"
        )

    @property
    def file_name(self):
        return self._file_name

    def get_local_path(self):
        print(
            f"Getting local path for OutputS3: {self._base_local_path}, {self._bucket}, {self._key}, {self._file_ext}, {self.remote_ow}"
        )
        return LocalPath(
            self._base_local_path,
            self._bucket,
            self._key,
            self._file_ext,
            self.remote_ow,
        )

    def __repr__(self):
        return f"OutputS3(bucket={self._bucket}, key={self._key}, file_ext={self._file_ext}, file_name={self._file_name}, remote_key_ow={self.remote_ow}, base_local_path={self._base_local_path})"


# Four operations: download file, download directory, upload file, upload directory (Multipart) to interact with pipeline files
class DataSource(ABC):
    def __init__(self):
        self.timings = []

    @abstractmethod
    def exists(self, path: S3Path) -> bool:
        pass

    @abstractmethod
    def download_file(self, read_path: S3Path, write_path: Path) -> None:
        pass

    @abstractmethod
    def download(self, read_path: S3Path, write_path: Path) -> None:
        pass

    @abstractmethod
    def upload(self, read_path: Path, write_path: Path) -> None:
        pass

    def write_parset_dict_to_file(self, parset_dict: dict, filename: str):
        with open(filename, "w") as f:
            for key, value in parset_dict.items():
                f.write(f"{key}={value}\n")

    def zip_without_compression(self, ms: LocalPath) -> LocalPath:
        logging.info(f"Starting zipping process for: {ms}")
        zip_filepath = LocalPath(
            ms.base_local_path, ms.bucket, ms.key + ".zip", ms.file_ext
        )

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
