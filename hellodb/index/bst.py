import sys

from hellodb.search_ds.bst import BSTree
from hellodb.index import Index


class BSTIndex(Index):
    def __init__(self):
        super().__init__()
        self._index = BSTree()

    def put(self, key, value):
        self._index.insert(key, value)

    def get(self, key):
        node = self._index.find(key)
        if node is not None:
            return node.value
        else:
            return None

    def contains(self, key):
        return self._index.find(key) is not None
