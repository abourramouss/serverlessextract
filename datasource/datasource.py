from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def download(self, key, output_file):
        pass
