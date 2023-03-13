import sys

from hellodb.search_ds.bst import BSTree
from hellodb.memstore import MemStore


class BSTMemStore(MemStore):
    def __init__(self):
        super().__init__()
        self._store = BSTree()

    def put(self, key, value):
        self._store.insert(key, value)

    def get(self, key):
        node = self._store.find(key)
        if node is not None:
            return node.value
        else:
            return None

    def delete(self, key):
        return self._store.delete(key)

    def contains(self, key):
        return self._store.find(key) is not None

    def size(self):
        return self._store.size

    def size_in_bytes(self):
        return sys.getsizeof(self._store)

    def get_all_pairs(self):
        for node in self._store.inorder(self._store.root):
            yield node.key, node.value


if __name__ == "__main__":
    mem_store = BSTMemStore()
    mem_store.put("test", "123")
    mem_store.put("test6", "123")
    mem_store.put("helloworld", "123")
    mem_store.put("123", "123")
    mem_store.put("helloworld1", "123")
    mem_store.put("test7", "123")
    mem_store.put("test4", "123")
    mem_store.put("test3", "123")
    mem_store.put("test5", "123")
    print(mem_store.size())
    mem_store.flush_to_disk()
