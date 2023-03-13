import os

from hellodb.utils import FileIOException


class DiskFile(object):
    def __init__(self, file_name, read_only, encoder, os_sync=True):
        self._wfh, self._rfh = None, None
        self._open(file_name, read_only)
        self._os_sync = os_sync
        self._encoder = encoder
        self._offset = os.stat(self.name).st_size

    def _open(self, file_name, read_only):
        if not read_only:
            self._wfh = open(file_name, "a+b")
        else:
            if not os.path.exists(file_name):
                raise FileIOException("file {} not found".format(file_name))
            self._rfh = open(file_name, "r+b")

    @property
    def file_handler(self):
        return self._wfh if self._wfh is not None else self._rfh

    @property
    def name(self):
        return self._wfh.name if self._wfh is not None else self._rfh.name

    @property
    def basename(self):
        return (
            os.path.basename(self._wfh.name)
            if self._wfh is not None
            else os.path.basename(self._rfh.name)
        )

    @property
    def size(self):
        return self._offset

    def close(self):
        if self._wfh is not None:
            self._wfh.close()
        else:
            self._rfh.close()

    def sync(self):
        if self._wfh is not None:
            os.fsync(self._wfh.fileno())
