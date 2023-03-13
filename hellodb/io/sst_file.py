from hellodb.consts import (
    CRC_FORMAT,
    SST_HEADER_FORMAT,
    SST_HEADER_SIZE,
    WHENCE_BEGINING,
)
from hellodb.io.disk_file import DiskFile
from hellodb.io.encoders import SSTFileEncoder
from hellodb.utils import FileIOException


class SSTFile(DiskFile):
    def __init__(self, file_name, read_only, os_sync=True):
        super().__init__(
            file_name,
            read_only,
            os_sync,
        )
        self._encoder = SSTFileEncoder(SST_HEADER_FORMAT, SST_HEADER_SIZE, CRC_FORMAT)

    def append(self, value):
        if self._wfh is None:
            raise FileIOException(
                "File {} is not opened in write mode".format(self.name)
            )
        entry_offset = self._offset
        entry = self._encoder.encode(value)
        data_len = self._wfh.write(entry)
        self._wfh.flush()
        if self._os_sync:
            self.sync()
        self._offset += data_len
        return entry_offset

    def read(self, offset):
        fh = self._wfh if self._wfh is not None else self._rfh
        fh.seek(offset, WHENCE_BEGINING)
        header = fh.read(self._encoder.header_size)
        crc, value_len = self._encoder.decode(header)
        value = fh.read(value_len)
        new_crc = self._encoder.calculate_checksum(header, value)
        if new_crc != crc:
            raise FileIOException("Mismatching CRC")
        return value.decode("utf-8")
