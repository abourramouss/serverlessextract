import os
import concurrent.futures
import shutil
import numpy as np
import hashlib

from casacore.tables import table
from radiointerferometry.datasource import LithopsDataSource, InputS3
from pathlib import PosixPath
from radiointerferometry.utils import get_dir_size, setup_logging

MB = 1024 * 1024


class StaticPartitioner:
    def __init__(self, log_level="INFO"):
        self.__log_level = log_level
        self.__logger = setup_logging(self.__log_level)
        self.__logger.info("Started StaticPartitioner")

    def __generate_concatenated_identifier(self, ms_tables, num_partitions):
        hash_md5 = hashlib.md5()

        total_rows = 0
        total_cols = 0
        ms_names = []

        for ms in ms_tables:
            total_rows += ms.nrows()
            total_cols = max(total_cols, ms.ncols())
            ms_names.append(ms.name())

        metadata = f"{total_rows}_{total_cols}_{num_partitions}_{'_'.join(ms_names)}"
        hash_md5.update(metadata.encode("utf-8"))
        identifier = hash_md5.hexdigest()
        identifier = identifier.strip("/")
        return identifier

    def __create_partition(self, i, start_row, end_row, ms, msout):
        self.datasource = LithopsDataSource()
        self.__logger.debug(
            f"Creating partition {i} with rows from {start_row} to {end_row - 1}..."
        )
        partition = ms.selectrows(list(range(start_row, end_row)))
        partition_name = PosixPath(f"partition_{i}.ms")
        partition.copy(str(partition_name), deep=True)
        partition.close()

        partition_size = get_dir_size(partition_name)
        self.__logger.debug(
            f"Partition {i} created. Size before zip: {partition_size} bytes"
        )

        zip_filepath = self.datasource.zip_without_compression(partition_name)
        self.__logger.debug(f"Partition {i} zipped at {zip_filepath}")

        # Get the file size before deleting the file
        zip_file_size = os.path.getsize(zip_filepath)
        self.__logger.debug(f"Zip file size: {zip_file_size} bytes")

        self.datasource.upload_file(zip_filepath, msout)

        os.remove(zip_filepath)
        shutil.rmtree(partition_name)

        self.__logger.info(f"Partition {i} uploaded.")

        return zip_filepath, partition_size

    def partition_ms(self, msin, num_partitions, msout):
        self.__logger = setup_logging(self.__log_level)

        self.__logger.info(
            f"Starting partitioning of {msin} into {num_partitions} partitions..."
        )

        self.datasource = LithopsDataSource()
        ms_to_part = self.datasource.download_directory(msin, PosixPath("/tmp"))

        self.__logger.debug(f"Downloaded files to: {ms_to_part}")
        full_file_paths = [PosixPath(ms_to_part) / f for f in os.listdir(ms_to_part)]
        self.__logger.debug(f"Files ready to be processed: {full_file_paths}")

        mss = []
        for f_path in full_file_paths:
            if not f_path.exists():
                self.__logger.debug(f"File not found: {f_path}")
                continue
            self.__logger.debug(f"Processing file: {f_path}")
            unzipped_ms = self.datasource.unzip(f_path)
            self.__logger.debug(f"Unzipped contents at: {unzipped_ms}")

            ms_table = table(str(unzipped_ms), ack=False)
            mss.append(ms_table)
            self.__logger.info(
                f"Number of rows in the measurement set: {ms_table.nrows()}"
            )
        ms = table(mss)
        identifier = self.__generate_concatenated_identifier(mss, num_partitions)
        self.__logger.info(
            f"Unique identifier for concatenated measurement sets: {identifier}"
        )

        msout.key = f"{msout.key}{identifier}/"
        if not self.datasource.exists(msout):
            self.__logger.info(f"Number of rows in the measurement set: {ms.nrows()}")
            self.__logger.info(
                f"Number of columns in the measurement set: {ms.ncols()}"
            )

            ms_sorted = ms.sort("TIME")
            total_rows = ms_sorted.nrows()
            self.__logger.info(f"Total rows in the measurement set: {total_rows}")
            times = np.array(ms_sorted.getcol("TIME"))
            total_duration = times[-1] - times[0]
            self.__logger.debug(
                f"Total duration in the measurement set: {total_duration}"
            )

            chunk_duration = total_duration / num_partitions
            partitions_info = []
            start_time = times[0]
            partition_count = 0

            for i in range(num_partitions):
                if i < num_partitions - 1:
                    end_time = start_time + chunk_duration
                    end_index = np.searchsorted(times, end_time, side="left")
                else:
                    end_index = total_rows
                start_index = np.searchsorted(times, start_time, side="left")
                partitions_info.append((partition_count, start_index, end_index))
                start_time = end_time
                partition_count += 1

            partition_sizes = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        self.__create_partition,
                        info[0],
                        info[1],
                        info[2],
                        ms_sorted,
                        msout,
                    )
                    for info in partitions_info
                ]
                for future in concurrent.futures.as_completed(futures):
                    partition_file, partition_size = future.result()
                    partition_sizes.append(partition_size)
                    self.__logger.debug(
                        f"Partition file {partition_file} with size {partition_size / MB:.2f} MB created and ready for upload."
                    )

            total_partition_size = sum(partition_sizes)
            self.__logger.debug(
                f"Total size of all partitions: {total_partition_size / MB:.2f} MB"
            )

            ms_sorted.close()
            self.__logger.debug(
                f"Partitioning completed. {len(partition_sizes)} partitions created."
            )
        else:
            self.__logger.info(
                f"Partitions already exist in {msout.bucket}/{msout.key}. Skipping partitioning."
            )
            return InputS3(bucket=msout.bucket, key=msout.key)
