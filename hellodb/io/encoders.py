import binascii
import struct

from hellodb import consts


class WalFileEncoder(object):
    def __init__(self, wal_header_format, wal_header_size, crc_format):
        self.header_format = wal_header_format
        self.header_size = wal_header_size
        self.crc_format = crc_format

    def encode(self, key, value):
        header = struct.pack(self.header_format, 0, len(key), len(value))
        key = str.encode(key)
        value = str.encode(value)
        crc = self.calculate_checksum(header, key, value)
        return struct.pack(self.crc_format, crc) + header[4:] + key + value

    def decode(self, value_bytes):
        crc, key_len, value_len = struct.unpack(
            self.header_format, value_bytes[: self.header_size]
        )
        return crc, key_len, value_len

    def calculate_checksum(self, header, key, value):
        crc = binascii.crc32(
            header[consts.CRC_SIZE :]
        )  # skip crc field i.e. first four bytes
        crc = binascii.crc32(key, crc)
        crc = binascii.crc32(value, crc)
        return crc


class SSTFileEncoder(object):
    def __init__(self, sst_header_format, sst_header_size, crc_format):
        self.header_format = sst_header_format
        self.header_size = sst_header_size
        self.crc_format = crc_format

    def encode(self, value):
        header = struct.pack(self.header_format, 0, len(value))
        value = str.encode(value)
        crc = self.calculate_checksum(header, value)
        return struct.pack(self.crc_format, crc) + header[4:] + value

    def decode(self, value_bytes):
        crc, value_len = struct.unpack(
            self.header_format, value_bytes[: self.header_size]
        )
        return crc, value_len

    def calculate_checksum(self, header, value):
        crc = binascii.crc32(
            header[consts.CRC_SIZE :]
        )  # skip crc field i.e. first four bytes
        crc = binascii.crc32(value, crc)
        return crc


class IndexFileEncoder(WalFileEncoder):
    pass
