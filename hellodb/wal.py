import binascii
import logging
import os
import struct
from threading import Lock, RLock

from hellodb.consts import (
    CRC_FORMAT,
    WAL_FILE_NAME_FORMAT,
    WAL_HEADER_FORMAT,
    WAL_HEADER_SIZE,
    WALFILE_START_INDEX,
)
from hellodb.logger import CustomAdapter
from hellodb import utils


class WalIOException(Exception):
    pass


def calculate_checksum(header, key, value):
    crc = binascii.crc32(header[4:])  # skip crc field i.e. first four bytes
    crc = binascii.crc32(key, crc)
    crc = binascii.crc32(value, crc)
    return crc


class WallFileEncoder(object):
    def __init__(self, wal_header_format, wal_header_size, crc_format):
        self.wal_header_format = wal_header_format
        self.wal_header_size = wal_header_size
        self.crc_format = crc_format

    def encode(self, key, value):
        header = struct.pack(self.wal_header_format, 0, len(key), len(value))
        key = str.encode(key)
        value = str.encode(value)
        crc = calculate_checksum(header, key, value)
        return struct.pack(self.crc_format, crc) + header[4:] + key + value

    def decode(self, value_bytes):
        crc, key_len, value_len = struct.unpack(
            self.wal_header_format, value_bytes[: self.wal_header_size]
        )
        return crc, key_len, value_len


class WalFileAppender(object):
    def __init__(self, file_name, encoder):
        self._wfh = None
        self._open(file_name)
        self._lock = Lock()
        self._encoder = encoder
        self._offset = os.stat(self.name).st_size

    def _open(self, file_name):
        self._wfh = open(file_name, "a+b")

    @property
    def file_handler(self):
        return self._wfh

    @property
    def file_id(self):
        return self._id

    @property
    def name(self):
        return self._wfh.name

    @property
    def basename(self):
        return os.path.basename(self._wfh.name)

    @property
    def size(self):
        with self._lock:
            return self._offset

    def close(self):
        self._wfh.close()

    def sync(self):
        os.fsync(self._wfh.fileno())

    def append(self, key, value):
        entry = self._encoder.encode(key, value)
        data_len = self._wfh.write(entry)
        self._wfh.flush()
        self.sync()
        self._offset += data_len


class WalFileReplayer(object):
    def __init__(self, wal_file, encoder):
        self._rfh = None
        self._file_name = wal_file
        self._open(self._file_name)
        self._encoder = encoder
        self._offset = os.stat(self.name).st_size

    def _open(self, wal_file):
        file_name = os.path.join(wal_file)
        if not os.path.exists(file_name):
            raise WalIOException("file {} not found".format(file_name))
        self._rfh = open(file_name, "r+b")

    @property
    def file_id(self):
        return self._file_name.split(".")[0]

    @property
    def name(self):
        return self._rfh.name

    @property
    def basename(self):
        return os.path.basename(self._rfh.name)

    @property
    def size(self):
        return self._offset

    def close(self):
        self._rfh.close()

    def replay(self):
        header = self._rfh.read(self._encoder.wal_header_size)
        while header:
            existing_crc, key_size, value_size = self._encoder.decode(header)
            key = self._rfh.read(key_size)
            value = self._rfh.read(value_size)
            crc = calculate_checksum(header, key, value)
            if crc != existing_crc:
                raise WalIOException("Mismatching CRC")
            yield key.decode("utf-8"), value.decode("utf-8")
            header = self._rfh.read(self._encoder.wal_header_size)


class WalManager(object):
    def __init__(self, file_path, max_file_size=100 * 1000 * 1000):
        self._file_path = file_path
        self._wal_file = None
        self._next_id = self._get_next_id()
        self._max_file_size = max_file_size
        self._read_files = {}
        self._lock = RLock()
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("WALMNGR")},
        )

    def _get_next_id(self):
        wal_files = utils.get_walfiles(self._file_path)
        if not wal_files:
            return WALFILE_START_INDEX
        else:
            return utils.get_file_id_from_absolute_path(wal_files[-1]) + 1

    def _close_current_write_files(self):
        if self._wal_file is not None:
            self._wal_file.close()
            self._wal_file = None

    def _create_new_wal_file(self, file_id):
        self._wal_file = WalFileAppender(
            os.path.join(self._file_path, WAL_FILE_NAME_FORMAT.format(file_id)),
            WallFileEncoder(WAL_HEADER_FORMAT, WAL_HEADER_SIZE, CRC_FORMAT),
        )

    def _rotate_files(self):
        self._next_id += 1
        self._close_current_write_files()
        self._wal_file = self._create_new_wal_file(self._next_id)

    def _check_write(self, data_len):
        with self._lock:
            if self._wal_file is None:
                self._create_new_wal_file(self._next_id)
            elif self._wal_file.size + WAL_HEADER_SIZE + data_len > self._max_file_size:
                self._rotate_files()

    def append(self, key, value):
        self._check_write(len(key) + len(value))
        self._wal_file.append(key, value)

    def replay(self):
        wal_files = utils.get_walfiles(self._file_path)
        if not wal_files:
            self.logger.debug("No WAL files found")
            return []
        for wal in wal_files:
            wal_replayer = WalFileReplayer(
                wal, WallFileEncoder(WAL_HEADER_FORMAT, WAL_HEADER_SIZE, CRC_FORMAT)
            )
            for key, value in wal_replayer.replay():
                yield key, value


if __name__ == "__main__":
    wal_mngr = WalManager(".")
    wal_mngr.append("test", "value")
    wal_mngr.append("test1", "value")
    for key, value in wal_mngr.replay():
        print(key, value)
