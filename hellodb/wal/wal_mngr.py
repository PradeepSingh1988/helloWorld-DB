import logging
import os
from threading import Lock

from hellodb.consts import (
    WAL_FILE_NAME_FORMAT,
    FILE_START_INDEX,
)
from hellodb.logger import CustomAdapter
from hellodb.io import reader, writer
from hellodb import utils


class WalManager(object):
    def __init__(self, file_path, max_entries=5000):
        self._file_path = file_path
        self._wal_file = None
        self._next_id = self._get_next_id()
        self._max_entries = max_entries
        self._read_files = {}
        self._lock = Lock()
        self._current_entries = 0
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("WALMNGR")},
        )

    def _get_next_id(self):
        wal_files = utils.get_walfiles(self._file_path)
        if not wal_files:
            return FILE_START_INDEX
        else:
            return utils.get_file_id_from_absolute_path(wal_files[-1]) + 1

    def _close_current_write_files(self):
        if self._wal_file is not None:
            self._wal_file.close()
            self._wal_file = None

    def _create_new_wal_file(self, file_id):
        self._wal_file = writer.WalWriter(
            os.path.join(self._file_path, WAL_FILE_NAME_FORMAT.format(file_id))
        )

    def _rotate_files(self):
        self._next_id += 1
        self._close_current_write_files()
        self._wal_file = self._create_new_wal_file(self._next_id)

    def _check_write(self, data_len):
        with self._lock:
            if self._wal_file is None:
                self._create_new_wal_file(self._next_id)
            elif self._current_entries + 1 > self._max_entries:
                self._rotate_files()

    def append(self, key, value):
        self._check_write(len(key) + len(value))
        self._current_entries += 1
        return self._wal_file.append(key, value)

    def replay(self):
        wal_files = utils.get_walfiles(self._file_path)
        if not wal_files:
            self.logger.debug("No WAL files found")
            return []
        for wal in wal_files:
            wal_replayer = reader.WalReader(wal)
            for key, value in wal_replayer.read_all():
                yield key, value

    def rotate(self):
        wal_path = self._wal_file.name
        self._rotate_files()
        return wal_path


if __name__ == "__main__":
    wal_mngr = WalManager(".")
    wal_mngr.append("test", "value")
    wal_mngr.append("test1", "value")
    for key, value in wal_mngr.replay():
        print(key, value)
