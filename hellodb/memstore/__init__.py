from abc import ABC, abstractmethod


class MemStore(ABC):
    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def delete(self, key):
        pass

    @abstractmethod
    def contains(self, key):
        pass

    @abstractmethod
    def size(self):
        pass

    @abstractmethod
    def size_in_bytes(self):
        pass

    @abstractmethod
    def flush_to_disk(self, disk_writer):
        pass
