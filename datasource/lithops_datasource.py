from lithops import Storage

from .datasource import DataSource
class LithopsDataSource(DataSource):
    def __init__(self):
        self.storage = Storage()


    def download(self, bucket, key, output_file):
        data = self.storage.get_object(bucket, key)
        content = data.decode('utf-8')


        with open(output_file, 'wb') as f:
            f.write(content)