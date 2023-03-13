from hellodb.io.index_file import IndexFile
from hellodb.io.sst_file import SSTFile
from hellodb.io.wal_file import WalFile


class WalWriter(object):
    def __init__(self, file_name):
        self._file = WalFile(
            file_name,
            False,
            True,
        )

    @property
    def name(self):
        return self._file.name

    @property
    def basename(self):
        return self._file.basename

    @property
    def size(self):
        return self._file.size

    def close(self):
        self._file.close()

    def append(self, key, value):
        offset = self._file.append(key, value)
        return offset


class IndexWriter(object):
    def __init__(self, file_name):
        self._file = IndexFile(
            file_name,
            False,
            True,
        )

    @property
    def name(self):
        return self._file.name

    @property
    def basename(self):
        return self._file.basename

    @property
    def size(self):
        return self._file.size

    def close(self):
        self._file.close()

    def append(self, key, value):
        offset = self._file.append(key, value)
        return offset


class SSTWriter(object):
    def __init__(self, file_name):
        self._file = SSTFile(
            file_name,
            False,
            True,
        )

    @property
    def name(self):
        return self._file.name

    @property
    def basename(self):
        return self._file.basename

    @property
    def size(self):
        return self._file.size

    def close(self):
        self._file.close()

    def append(self, value):
        offset = self._file.append(value)
        return offset
