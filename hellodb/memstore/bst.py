import sys

from hellodb.memstore import MemStore


class Node(object):
    def __init__(self, key, value, left_child=None, right_child=None):
        self.key = key
        self.value = value
        self.left_child = left_child
        self.right_child = right_child

    def __repr__(self):
        return "Node({})".format(self.key)

    def __eq__(self, other):
        return self.key == other.key


class BSTree(object):
    def __init__(self):
        self.root = None
        self.size = 0

    def _find(self, key):
        if self.root is None:
            return ValueError("Tree is not initialised")
        current_node = self.root
        is_left_child = False
        parent = None
        while current_node.key != key:
            parent = current_node
            if key < current_node.key:
                current_node = current_node.left_child
                is_left_child = True
            else:
                current_node = current_node.right_child
                is_left_child = False
            if current_node is None:
                return None, None, None
        return current_node, is_left_child, parent

    def find(self, key):
        return self._find(key)[0]

    def insert(self, key, value):
        if self.root is None:
            self.root = Node(key, value)
            self.size += 1
        else:
            current = self.root
            parent = None
            while True:
                parent = current
                if key == current.key:
                    current.value = value
                elif key < current.key:
                    current = current.left_child
                    if current == None:
                        parent.left_child = Node(key, value)
                        self.size += 1
                        break
                else:
                    current = current.right_child
                    if current == None:
                        parent.right_child = Node(key, value)
                        self.size += 1
                        break

    def inorder(self, node):
        if node != None:
            self.inorder(node.left_child)
            yield node
            self.inorder(node.right_child)

    def preorder(self, node):
        if node != None:
            yield node
            self.inorder(node.left_child)
            self.inorder(node.right_child)

    def postorder(self, node):
        if node != None:
            self.postorder(node.left_child)
            self.postorder(node.right_child)
            yield node

    def max(self):
        current = self.root
        previous = None
        while current != None:
            previous = current
            current = current.right_child
        return previous

    def min(self):
        current = self.root
        previous = None
        while current != None:
            previous = current
            current = current.left_child
        return previous

    def _successor(self, node):
        successor = node
        parent = None
        current = node.right_child
        while current is not None:
            parent = successor
            successor = current
            current = current.left_child
        return successor, parent

    def _is_leaf(self, node):
        return node.left_child is None and node.right_child is None

    def _delete_leaf_node(self, node, is_left_child, parent):
        if node == self.root:
            self.root = None
        elif is_left_child:
            parent.left_child = None
        else:
            parent.right_child = None

    def _delete_node_with_one_child(self, node, is_left_child, parent):
        if node.right_child is None:
            if node == self.root:
                self.root = node.left_child
            elif node == node.parent.left_child:
                parent.left_child = node.left_child
            else:
                parent.right_child = node.left_child
        elif node.left_child is None:
            if node == self.root:
                self.root = node.right_child
            elif is_left_child:
                parent.left_child = node.right_child
            else:
                parent.right_child = node.right_child

    def delete(self, key):
        node, is_left_child, parent = self._find(key)
        if node is None:
            return False
        if self._is_leaf(node):
            self._delete_leaf_node(node, is_left_child, parent)
        elif node.left_child is None or node.right_child is None:
            self._delete_node_with_one_child(node, is_left_child, parent)
        else:
            successor, successor_parent = self._successor(node)
            if successor != node.right_child:
                successor_parent.left_child = successor.right_child
                successor.right_child = node.right_child
            if node == self.root:
                root = successor
            elif is_left_child:
                parent.left_child = successor
            else:
                parent.right_child = successor

            successor.left_child = node.left_child
        return True


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
        self._store.delete(key)

    def contains(self, key):
        return self._store.find(key) is not None

    def size(self):
        return len(self._store.size)

    def size_in_bytes(self):
        return sys.getsizeof(self._store)

    def flush_to_disk(self, disk_writer):
        for node in self._store.inorder():
            disk_writer.write(node.key, node.value)
