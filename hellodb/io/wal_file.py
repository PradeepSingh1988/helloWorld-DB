from hellodb.consts import (
    CRC_FORMAT,
    WAL_HEADER_FORMAT,
    WAL_HEADER_SIZE,
    WHENCE_BEGINING,
)
from hellodb.io.disk_file import DiskFile
from hellodb.io.encoders import WalFileEncoder
from hellodb.utils import FileIOException


class WalFile(DiskFile):
    def __init__(self, file_name, read_only, os_sync=True):
        super().__init__(
            file_name,
            read_only,
            WalFileEncoder(WAL_HEADER_FORMAT, WAL_HEADER_SIZE, CRC_FORMAT),
            os_sync,
        )

    def append(self, key, value):
        if self._wfh is None:
            raise FileIOException(
                "File {} is not opened in write mode".format(self.name)
            )
        entry_offset = self._offset
        entry = self._encoder.encode(key, value)
        data_len = self._wfh.write(entry)
        self._wfh.flush()
        if self._os_sync:
            self.sync()
        self._offset += data_len
        return entry_offset

    def read_all_entries(self):
        fh = self._wfh if self._wfh is not None else self._rfh
        header = fh.read(self._encoder.header_size)
        while header:
            existing_crc, key_size, value_size = self._encoder.decode(header)
            key = self._rfh.read(key_size)
            value = self._rfh.read(value_size)
            crc = self._encoder.calculate_checksum(header, key, value)
            if crc != existing_crc:
                raise FileIOException("Mismatching CRC")
            yield key.decode("utf-8"), value.decode("utf-8")
            header = self._rfh.read(self._encoder.header_size)
