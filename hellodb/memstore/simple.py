import sys


from hellodb.memstore import MemStore


class SimpleMemStore(MemStore):
    def __init__(self):
        super().__init__()
        self._store = {}

    def put(self, key, value):
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key)

    def contains(self, key):
        return key in self._store

    def size(self):
        return len(self._store.keys())

    def size_in_bytes(self):
        return sys.getsizeof(self._store)

    def flush_to_disk(self, disk_writer):
        sorted_memstore = sorted(self._store.items(), key=lambda x: x[0])
        for key, value in sorted_memstore:
            disk_writer.write(key, value)
