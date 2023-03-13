from hellodb.memstore.bst import BSTMemStore


class RWMemstore(object):
    def __init__(self):
        self.ro_memstore = BSTMemStore()
        self.wo_memstore = BSTMemStore()

    def contains(self, key):
        return self.wo_memstore.contains(key) or self.ro_memstore.contains(key)

    def get(self, key):
        value = self.wo_memstore.get(key)
        return value if value else self.ro_memstore.get(key)

    def put(self, key, value):
        return self.wo_memstore.put(key, value)

    def delete(self, key):
        # implement later
        pass

    def size(self):
        return self.wo_memstore.size()

    def switch_stores(self):
        self.ro_memstore = self.wo_memstore
        self.wo_memstore = BSTMemStore()
