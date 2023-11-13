import os
from lithops import Storage
from multiprocessing import Pool


def upload_file(file_info):
    local_file, bucket, s3_key = file_info
    storage = Storage()
    try:
        print(f"Uploading {local_file} to {bucket}/{s3_key}...")
        storage.upload_file(local_file, bucket, key=s3_key)
        print(f"Upload finished for {local_file}")
    except Exception as e:
        print(f"An exception occurred: {e}")


def upload_directory_to_s3(local_directory, bucket, s3_prefix):
    file_list = []
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.join(
                s3_prefix, os.path.relpath(local_file_path, local_directory)
            )
            file_list.append((local_file_path, bucket, s3_key))

    for file in file_list:
        upload_file(file)
