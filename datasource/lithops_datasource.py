from lithops import Storage
import os
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3path import S3Path
from pathlib import PosixPath


KB = 1024
MB = KB * KB


def s3_to_local_path(
    s3_path: S3Path, base_local_dir: PosixPath = PosixPath("/tmp")
) -> PosixPath:
    """Converts an S3Path to a local file path."""
    local_path = os.path.join(base_local_dir, s3_path.bucket, s3_path.key)
    return PosixPath(local_path)


def local_to_s3_path(local_path: str, base_local_dir: str = "/tmp") -> S3Path:
    """Converts a local file path to an S3Path."""
    local_path = os.path.abspath(local_path)
    components = local_path.replace(base_local_dir, "").split(os.path.sep)[1:]
    bucket = components[0]
    key = "/".join(components[1:])
    return S3Path(f"{bucket}/{key}")


class LithopsDataSource(DataSource):
    def __init__(
        self,
    ):
        self.storage = Storage()

    def download_file(
        self, read_path: S3Path, base_path: PosixPath = PosixPath("/tmp")
    ) -> PosixPath:
        if isinstance(read_path, S3Path):
            try:
                local_path = s3_to_local_path(read_path, base_local_dir=str(base_path))
                os.makedirs(local_path.parent, exist_ok=True)

                # Download file uses the default config
                self.storage.download_file(
                    read_path.bucket,
                    read_path.key,
                    str(local_path),
                )

                return PosixPath(local_path)
            except Exception as e:
                print(f"Failed to download file {read_path.key}: {e}")

    def download_directory(
        self, read_path: S3Path, base_path: PosixPath = PosixPath("/tmp")
    ) -> PosixPath:
        """Download a directory from S3 and returns the local path."""
        keys = self.storage.list_keys(read_path.bucket, prefix=read_path.key)
        print(keys)
        local_directory_path = s3_to_local_path(
            read_path, base_local_dir=str(base_path)
        )

        for key in keys:
            s3_file_path = S3Path.from_bucket_key(read_path.bucket, key)
            self.download_file(
                s3_file_path, base_path=base_path
            )  # Using the same base_path for file downloads

        return PosixPath(local_directory_path)

    def upload_file(self, read_path: PosixPath, write_path: S3Path) -> None:
        """Uploads a local file to S3."""
        try:
            self.storage.upload_file(str(read_path), write_path.bucket, write_path.key)
        except Exception as e:  # Consider narrowing down the exceptions caught.
            print(f"Failed to upload file {read_path} to {write_path}. Error: {e}")

    def upload_directory(self, read_path: PosixPath, write_base_path: S3Path) -> None:
        """Uploads a local directory to S3, maintaining its structure."""
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []

            for dirpath, _, filenames in os.walk(read_path):
                for filename in filenames:
                    local_file_path = PosixPath(dirpath).joinpath(filename)
                    relative_path = local_file_path.relative_to(read_path)
                    s3_path = write_base_path.joinpath(relative_path)

                    futures.append(
                        executor.submit(self.upload_file, local_file_path, s3_path)
                    )

            for future in as_completed(futures):
                future.result()
