from hellodb.consts import (
    CRC_FORMAT,
    IDX_HEADER_FORMAT,
    IDX_HEADER_SIZE,
    WHENCE_BEGINING,
)
from hellodb.io.encoders import IndexFileEncoder
from hellodb.io.wal_file import WalFile


class IndexFile(WalFile):
    def __init__(self, file_name, read_only, os_sync=True):
        super().__init__(
            file_name,
            read_only,
            os_sync,
        )
        self._encoder = IndexFileEncoder(IDX_HEADER_FORMAT, IDX_HEADER_SIZE, CRC_FORMAT)

    def append(self, key, offset):
        offset = str(offset)
        return super().append(key, offset)

    def read_all_entries(self):
        for key, offset in super().read_all_entries():
            yield key, int(offset)
