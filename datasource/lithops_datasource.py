from lithops import Storage
import os
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import PosixPath
from datasource import InputS3, OutputS3

KB = 1024
MB = KB * KB


def s3_to_local_path(
    s3_path: InputS3, base_local_dir: PosixPath = PosixPath("/tmp")
) -> PosixPath:
    """Converts an S3Path to a local file path."""
    local_path = os.path.join(base_local_dir, s3_path.bucket, s3_path.key)
    return PosixPath(local_path)


def local_to_s3_path(local_path: str, base_local_dir: str = "/tmp"):
    """Converts a local file path to an S3Path."""
    local_path = os.path.abspath(local_path)
    components = local_path.replace(base_local_dir, "").split(os.path.sep)[1:]
    bucket = components[0]
    key = "/".join(components[1:])
    return OutputS3(bucket=bucket, key=key)


class LithopsDataSource(DataSource):
    def __init__(
        self,
    ):
        self.storage = Storage()

    def download_file(
        self, read_path: InputS3, base_path: PosixPath = PosixPath("/tmp")
    ):
        if isinstance(read_path, InputS3):
            try:
                local_path = s3_to_local_path(read_path, base_local_dir=str(base_path))
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
        print(keys)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(base_path)
        )

        for key in keys:
            s3_file_path = InputS3(bucket=read_path.bucket, key=key)
            self.download_file(
                s3_file_path, base_path=base_path
            )  # Using the same base_path for file downloads

        return local_directory_path

    def upload_file(self, local_path: PosixPath, output_s3: OutputS3, id=None):
        """Uploads a single file to an S3 bucket based on the OutputS3 configuration."""
        s3_path = output_s3.construct_s3_path(id=id)
        bucket, key = s3_path.split("/", 1)
        try:
            self.storage.upload_file(str(local_path), bucket, key)
        except Exception as e:
            print(f"Failed to upload file {local_path} to {output_s3}. Error: {e}")

    def upload_directory(
        self, local_directory: PosixPath, output_s3: OutputS3, id=None
    ):
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
                        executor.submit(
                            self.upload_file, local_file_path, future_s3, id=id
                        )
                    )
            for future in as_completed(futures):
                future.result()
