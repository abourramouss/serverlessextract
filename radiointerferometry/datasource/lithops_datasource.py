import os

from lithops import Storage
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import PosixPath
from radiointerferometry.datasource import InputS3, OutputS3
from radiointerferometry.profiling import time_it

KB = 1024
MB = KB * KB


def s3_to_local_path(
    s3_path: InputS3, base_local_dir: PosixPath = PosixPath("/tmp")
) -> PosixPath:
    local_path = os.path.join(base_local_dir, s3_path.bucket, f"{s3_path.key}/")

    return PosixPath(local_path)


def local_path_to_s3(
    local_path: PosixPath, base_local_dir: PosixPath = PosixPath("/tmp")
) -> OutputS3:

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

    def download_file(
        self, read_path: InputS3, base_path: PosixPath = PosixPath("/tmp")
    ):
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

    def download_directory(
        self, read_path: InputS3, base_path: PosixPath = PosixPath("/tmp")
    ):
        """Download a directory from S3 and returns the local path."""
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(base_path)
        )

        for key in keys:
            s3_file_path = InputS3(bucket=read_path.bucket, key=key)
            self.download_file(s3_file_path, base_path=base_path)

        return local_directory_path

    def upload_file(self, local_path: PosixPath, output_s3: OutputS3):
        """Uploads a single file to an S3 bucket based on the OutputS3 configuration."""
        if not os.path.isfile(local_path):
            raise ValueError(f"The path {local_path} is not a file.")
        bucket = output_s3.bucket
        key = os.path.join(output_s3.key, local_path.name)
        try:
            self.storage.upload_file(str(local_path), bucket, key)
        except Exception as e:
            print(e)

    def upload_directory(self, local_directory: PosixPath, output_s3: OutputS3):
        """Uploads all files within a local directory to an S3 path, maintaining structure."""
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for dirpath, _, filenames in os.walk(str(local_directory)):
                for filename in filenames:
                    local_file_path = PosixPath(dirpath).joinpath(filename)
                    relative_path = local_file_path.relative_to(str(local_directory))
                    full_s3_path = os.path.join(
                        output_s3.key, str(relative_path)
                    ).replace(os.sep, "/")
                    future_s3 = OutputS3(
                        output_s3.bucket, full_s3_path, output_s3.naming_pattern
                    )
                    futures.append(
                        executor.submit(self.upload_file, local_file_path, future_s3)
                    )
            for future in as_completed(futures):
                future.result()
