from abc import ABC, abstractmethod


class Index(ABC):
    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def contains(self, key):
        pass
