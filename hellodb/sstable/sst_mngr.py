import os
from threading import RLock

from hellodb.consts import (
    IDX_FILE_NAME_FORMAT,
    SST_FILE_NAME_FORMAT
)
from hellodb.index import bst
from hellodb.io import reader, writer
from hellodb import utils


class SSTableROMngr(object):
    def __init__(self, sst_file_path, sst_file_name):
        self._index = bst.BSTIndex()
        self._sst_reader = reader.SSTReader(
            os.path.join(sst_file_path, SST_FILE_NAME_FORMAT.format(sst_file_name)),
        )
        self._index_reader = reader.IndexReader(
            os.path.join(sst_file_path, IDX_FILE_NAME_FORMAT.format(sst_file_name))
        )
        self._load_index()

    def _load_index(self):
        for key, offset in self._index_reader.read_all():
            self._index.put(key, int(offset))

    def contains(self, key):
        return self._index.contains(key)

    def get(self, key):
        offset = self._index.get(key)
        if offset is not None:
            value = self._sst_reader.read(offset)
            return value
        else:
            return None

    def close(self):
        self._index_reader.close()
        self._sst_reader.close()


class SSTableRWMngr(object):
    def __init__(self, sst_file_path, sst_file_name):
        self._index = bst.BSTIndex()
        self._sst_writer = writer.SSTWriter(
            os.path.join(sst_file_path, SST_FILE_NAME_FORMAT.format(sst_file_name)),
        )
        self._index_writer = writer.IndexWriter(
            os.path.join(sst_file_path, IDX_FILE_NAME_FORMAT.format(sst_file_name))
        )

    def write_key_value(self, key, value):
        offset = self._sst_writer.append(value)
        self._index_writer.append(key, offset)

    def close(self):
        self._index_writer.close()
        self._sst_writer.close()


class SSTableCollection(object):
    def __init__(self, sst_readers):
        self._sst_readers = sst_readers

    def contains(self, key):
        for index in range(len(self._sst_readers) - 1, -1, -1):
            if self._sst_readers[index].contains(key):
                return True
        return False

    def get(self, key):
        for index in range(len(self._sst_readers) - 1, -1, -1):
            value = self._sst_readers[index].get(key)
            if value is not None:
                return value


class SSTableManager(object):
    def __init__(self):
        self._lock = RLock()
        self._all_sst_readers = []
        self._current_reader = None

    def add_reader(self, sst_reader):
        with self._lock:
            self._all_sst_readers.append(sst_reader)
            self._current_reader = SSTableCollection(self._all_sst_readers)

    def clear_reders(self):
        with self._lock:
            self._all_sst_readers = []
            self._current_reader = None

    def get_current_reader(self):
        with self._lock:
            return self._current_reader
