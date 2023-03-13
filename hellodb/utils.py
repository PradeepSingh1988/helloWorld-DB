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


def remove_all_wal_files(file_path):
    wal_files = get_walfiles(file_path)
    for wal_file in wal_files:
        os.remove(wal_file)
