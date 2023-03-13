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
        return False if self._store.pop(key, None) is None else True

    def contains(self, key):
        return key in self._store

    def size(self):
        return len(self._store.keys())

    def size_in_bytes(self):
        return sys.getsizeof(self._store)

    def flush_to_disk(self):
        sorted_memstore = sorted(self._store.items(), key=lambda x: x[0])
        for key, value in sorted_memstore:
            print(key, value)


if __name__ == "__main__":
    mem_store = SimpleMemStore()
    mem_store.put("test", "123")
    mem_store.put("test1", "123")
    mem_store.put("helloworld", "123")
    mem_store.put("123", "123")
    mem_store.flush_to_disk()
