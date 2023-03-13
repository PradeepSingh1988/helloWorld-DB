from hellodb.io.index_file import IndexFile
from hellodb.io.sst_file import SSTFile
from hellodb.io.wal_file import WalFile


class WalReader(object):
    def __init__(self, file_path):
        self._file = WalFile(
            file_path,
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

    def read_all(self):
        for key, value in self._file.read_all_entries():
            yield key, value


class IndexReader(object):
    def __init__(self, file_path):
        self._file = IndexFile(
            file_path,
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

    def read_all(self):
        for key, value in self._file.read_all_entries():
            yield key, value


class SSTReader(object):
    def __init__(self, file_path):
        self._file = SSTFile(
            file_path,
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

    def read(self, offset):
        return self._file.read(offset)
