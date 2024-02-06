from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import Union
from s3path import S3Path
import zipfile
import os


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

    def zip(self, ms: PosixPath) -> PosixPath:
        print(f"Zipping directory: {ms}")
        zip_filepath = PosixPath(
            f"{ms.parent}/{ms.name}.zip"
        )  # File path for the new zip file
        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(ms):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Include the partition folder name in the zip file
                    arcname = os.path.relpath(file_path, start=ms.parent)
                    zip_file.write(file_path, arcname)
        return zip_filepath

    def zip_files(self, ms: PosixPath, h5_file: PosixPath) -> PosixPath:
        partition_name = ms.name  # Assuming ms.name is something like 'partition_1.ms'
        zip_filepath = PosixPath(f"{ms.parent}/{partition_name}.zip")

        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Add .ms files to the zip, within a subfolder named 'partition_x.ms/ms'
            for root, dirs, files in os.walk(ms):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.join(
                        partition_name,
                        "ms",
                        os.path.relpath(file_path, start=ms),
                    )
                    zip_file.write(file_path, arcname)

            # Add .h5 file to the zip, within a subfolder named 'partition_x.ms/h5'
            h5_arcname = os.path.join(partition_name, "h5", h5_file.name)
            zip_file.write(h5_file, h5_arcname)

        return zip_filepath

    def unzip(self, ms: PosixPath) -> PosixPath:
        zip_file = zipfile.ZipFile(ms)
        extract_path = ms.parent
        zip_file.extractall(extract_path)
        zip_file.close()

        root_items = {item.split("/")[0] for item in zip_file.namelist()}
        if len(root_items) == 1:
            part_name = next(iter(root_items))
            new_ms_path = extract_path / part_name
        else:
            new_ms_path = extract_path

        print(f"Unzipped to: {new_ms_path}")
        return new_ms_path
