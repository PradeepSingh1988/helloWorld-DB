import binascii
import glob
import os
import struct

from hellodb import consts


class FileIOException(Exception):
    pass


def get_sstfiles(file_path):
    return sorted(
        glob.glob(os.path.join(file_path, consts.SST_FILE_NAME_FORMAT.format("*"))),
        key=lambda x: int(os.path.basename(x).split(".")[0]),
    )


def get_walfiles(file_path):
    return sorted(
        glob.glob(os.path.join(file_path, consts.WAL_FILE_NAME_FORMAT.format("*"))),
        key=lambda x: int(os.path.basename(x).split(".")[0]),
    )


def get_file_id_from_name(filename):
    return int(filename.split(".")[0])


def get_file_id_from_absolute_path(filepath):
    filename = os.path.basename(filepath)
    return get_file_id_from_name(filename)


def calculate_checksum(header, key, value):
    crc = binascii.crc32(
        header[consts.CRC_SIZE :]
    )  # skip crc field i.e. first four bytes
    crc = binascii.crc32(key, crc)
    crc = binascii.crc32(value, crc)
    return crc


class FileEncoder(object):
    def __init__(self, wal_header_format, wal_header_size, crc_format):
        self.header_format = wal_header_format
        self.header_size = wal_header_size
        self.crc_format = crc_format

    def encode(self, key, value):
        header = struct.pack(self.header_format, 0, len(key), len(value))
        key = str.encode(key)
        value = str.encode(value)
        crc = calculate_checksum(header, key, value)
        return struct.pack(self.crc_format, crc) + header[4:] + key + value

    def decode(self, value_bytes):
        crc, key_len, value_len = struct.unpack(
            self.header_format, value_bytes[: self.header_size]
        )
        return crc, key_len, value_len
