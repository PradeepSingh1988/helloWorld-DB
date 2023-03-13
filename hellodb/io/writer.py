from hellodb.io.disk_file import DiskFile


class Writer(object):
    def __init__(self, file_name, encoder):
        self._file = DiskFile(
            file_name,
            False,
            encoder,
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
