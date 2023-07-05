from abc import ABC, abstractmethod


class PartitionStrategy:
    @abstractmethod
    def partition(self, mss: list[str], num_chunks: int) -> list[str]:
        raise NotImplementedError("partition() must be implemented by a subclass")
