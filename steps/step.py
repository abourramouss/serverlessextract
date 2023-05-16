from abc import ABC, abstractmethod

class Step(ABC):
    @abstractmethod
    def run(self):
        pass
