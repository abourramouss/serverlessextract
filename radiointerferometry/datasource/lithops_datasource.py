import os

from lithops import Storage
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.profiling import time_it

KB = 1024
MB = KB * KB


def s3_to_local_path(s3_path: InputS3, base_local_dir: Path = Path("/tmp")) -> Path:
    local_path = os.path.join(base_local_dir, s3_path.bucket, f"{s3_path.key}/")
    return Path(local_path)


def local_path_to_s3(local_path: Path, base_local_dir: Path = Path("/tmp")) -> OutputS3:

    if not local_path.is_absolute():
        raise ValueError("local_path must be an absolute path")

    try:
        relative_path = local_path.relative_to(base_local_dir)
    except ValueError:
        raise ValueError("local_path is not a child of the base_local_dir")

    parts = relative_path.parts
    bucket = parts[0]
    file_name_with_ext = parts[-1]
    file_name, file_ext = os.path.splitext(file_name_with_ext)
    key = "/".join(parts[1:-1]) if len(parts) > 2 else ""
    return OutputS3(
        bucket=bucket, key=key, file_ext=file_ext.strip("."), file_name=file_name
    )


class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()
        self.time_records = []

    def exists(self, path: OutputS3) -> bool:
        """Check if a file exists in an S3 bucket."""
        return len(self.storage.list_keys(path.bucket, prefix=path.key)) > 0

    def download_file(self, read_path: InputS3, base_path: Path = Path("/tmp")):
        if isinstance(read_path, InputS3):
            try:
                local_path = s3_to_local_path(read_path, base_local_dir=str(base_path))

                if local_path.exists():
                    print(f"File {local_path} already exists locally.")
                    return local_path

                os.makedirs(local_path.parent, exist_ok=True)

                # Download file uses the default config
                self.storage.download_file(
                    read_path.bucket,
                    read_path.key,
                    str(local_path),
                )

                return local_path
            except Exception as e:
                print(f"Failed to download file {read_path.key}: {e}")

    def download(self, read_path: InputS3, base_path: Path = Path("/tmp")):
        """Download from S3 and returns the local path."""
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(base_path)
        )

        for key in keys:
            s3_file_path = InputS3(bucket=read_path.bucket, key=key)
            self.download_file(s3_file_path, base_path=base_path)

        return local_directory_path

    def upload(self, local_path: Path, output_s3: OutputS3):
        """Uploads a single file to an S3 bucket based on the OutputS3 configuration."""
        if not os.path.isfile(local_path):
            raise ValueError(f"The path {local_path} is not a file.")
        bucket = output_s3.bucket

        # Use remote_key_ow if it exists, otherwise use the original key
        if output_s3.remote_ow:
            key = os.path.join(output_s3.remote_ow, local_path.name)
        else:
            key = os.path.join(output_s3.key, local_path.name)

        print(local_path)
        print(f"Uploading to bucket: {bucket}, key: {key}")
        try:
            self.storage.upload_file(str(local_path), bucket, key)
        except Exception as e:
            print(e)
