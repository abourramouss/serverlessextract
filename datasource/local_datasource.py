import os
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import timeit_io
import shutil


class LocalDataSource(DataSource):
    def __init__(self):
        pass

    @timeit_io
    def download_file(self, source_dir, rel_file_path, dest_dir):
        try:
            source_file_path = os.path.join(source_dir, rel_file_path)
            dest_file_path = os.path.join(dest_dir, rel_file_path)
            os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
            shutil.copy(source_file_path, dest_file_path)
        except Exception as e:
            print(f"Failed to copy file {rel_file_path}: {e}")

    @timeit_io
    def download(self, source_dir: str, rel_dir: str, dest_dir: str) -> None:
        rel_dir_path = os.path.join(source_dir, rel_dir)
        files = [os.path.join(path, filename) for path, dirs, files in os.walk(rel_dir_path) for filename in files]
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(self.download_file, source_dir, file, dest_dir) for file in files]
        for future in as_completed(futures):
            future.result()

        total_size = 0
        for dirpath, dirnames, filenames in os.walk(dest_dir):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)

        print(f"Total size of copied files: {total_size / (1024 * 1024)} MB")

    @timeit_io
    def upload_file(self, source_dir, rel_file_path, dest_dir):
        self.download_file(source_dir, rel_file_path, dest_dir)

    @timeit_io
    def upload(self, source_dir: str, rel_dir: str, dest_dir: str) -> None:
        self.download(source_dir, rel_dir, dest_dir)

    def get_ms_size(self, directory):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)

        return total_size
