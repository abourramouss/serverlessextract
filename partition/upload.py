import os
import multiprocessing
from lithops import Storage

import os
import time
import multiprocessing
from lithops import Storage

def upload_file_to_s3(local_file, bucket, s3_key, progress_dict):
    storage = Storage()
    try:
        print(f'Starting upload for {local_file} to {bucket}/{s3_key}...')
        storage.upload_file(local_file, bucket, s3_key)
        progress_dict[local_file] = 'Finished'
    except Exception as e:
        progress_dict[local_file] = 'Failed'
        print(f'Failed to upload {local_file}. Reason: {str(e)}')

def print_progress(progress_dict):
    while True:
        time.sleep(1)  
        print(dict(progress_dict))

def upload_directory_to_s3_parallel(local_directory, bucket, s3_directory):
    files_to_upload = []
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_directory)
            s3_key = os.path.join(s3_directory, relative_path)
            files_to_upload.append((local_file, bucket, s3_key))

    manager = multiprocessing.Manager()
    progress_dict = manager.dict()

    progress_process = multiprocessing.Process(target=print_progress, args=(progress_dict,))
    progress_process.start()

    with multiprocessing.Pool() as pool:
        results = [pool.apply_async(upload_file_to_s3, args + (progress_dict,)) for args in files_to_upload]

    for result in results:
        result.wait()

    progress_process.terminate()
    progress_process.join()

    print('Final progress:')
    print(dict(progress_dict))

upload_directory_to_s3_parallel('/home/ayman/extract-project/Pipeline/src/partition/partitions', 'aymanb-serverless-genomics', 'extract-data/partitions')
