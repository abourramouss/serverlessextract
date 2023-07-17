from lithops import Storage
import os
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from util import timeit_io


class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()

    @timeit_io
    def download_file(self, bucket, key, output_dir):
        try:
            local_path = os.path.join(output_dir, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.storage.download_file(bucket, key, local_path)
        except Exception as e:
            print(f"Failed to download file {key}: {e}")

    @timeit_io
    def download(self, bucket: str, directory: str, write_dir: str) -> int:
        keys = self.storage.list_keys(bucket, prefix=directory)
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(self.download_file, bucket, key, write_dir)
                for key in keys
            ]
        for future in as_completed(futures):
            future.result()

        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)

        print(f"Total size of downloaded files: {total_size / (1024 * 1024)} MB")

    @timeit_io
    def upload_file(self, bucket, directory, abs_file_path, rel_file_path):
        try:
            key = f"{directory}/{rel_file_path}"
            self.storage.upload_file(abs_file_path, bucket, key)
        except Exception as e:
            print(f"Failed to upload file {rel_file_path}: {e}")

    @timeit_io
    def upload(self, bucket: str, s3_directory: str, local_directory: str) -> None:
        base_name = os.path.basename(local_directory)
        files = [
            (
                os.path.join(path, filename),
                os.path.join(
                    base_name,
                    os.path.relpath(os.path.join(path, filename), local_directory),
                ),
            )
            for path, dirs, files in os.walk(local_directory)
            for filename in files
        ]
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(
                    self.upload_file, bucket, s3_directory, file[0], file[1]
                )
                for file in files
            ]

        for future in as_completed(futures):
            future.result()

    def get_ms_size(self, bucket_name, directory):
        objects = self.storage.list_objects(bucket_name, prefix=directory)

        total_size = sum(obj["Size"] for obj in objects)

        return total_size
