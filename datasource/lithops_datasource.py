from lithops import Storage
import os
from .datasource import DataSource
from concurrent.futures import ThreadPoolExecutor, as_completed

class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()


    def download_file(self, storage, bucket, key, write_dir):
        try:
            file_body = storage.get_object(bucket, key, stream=True)
            local_path = os.path.join(write_dir, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                while True:
                    chunk = file_body.read(100000000)  # Read 100 mb.
                    if not chunk:
                        break
                    f.write(chunk)
                    
        except Exception as e:
            print(f"Failed to download file {key}: {e}")

    def download(self, bucket: str, directory: str, write_dir: str) -> None:
        keys = self.storage.list_keys(bucket, prefix=directory)
        
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.download_file, self.storage, bucket, key, write_dir) for key in keys]

        for future in as_completed(futures):
            future.result()
        
