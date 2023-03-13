from hellodb.io.disk_file import DiskFile


class Reader(object):
    def __init__(self, file_path, encoder):
        self._file = DiskFile(
            file_path,
            True,
            encoder,
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

    def read(self, offset):
        return self._file.read(offset)
