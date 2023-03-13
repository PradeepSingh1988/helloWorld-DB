import logging
import os
from queue import Queue
from threading import Lock, Thread
import time

from hellodb.consts import FILE_START_INDEX, SST_FILE_NAME_FORMAT, TOMBSTONE_ENTRY
from hellodb.logger import CustomAdapter, setup_logger
from hellodb.memstore.rw_memstore import RWMemstore
from hellodb.sstable.sst_mngr import SSTableManager, SSTableROMngr, SSTableRWMngr
from hellodb import utils
from hellodb.wal.wal_mngr import WalManager


setup_logger()


class HelloDB(object):
    def __init__(self, file_path, memstore_max_size):
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("HelloDB")},
        )
        self._file_path = file_path
        self._next_id = self._get_next_id()
        self._memstore_max_size = memstore_max_size
        self._lock = Lock()
        self._sst_mngr = SSTableManager()
        self._rw_memstore = RWMemstore()
        self._wal_mngr = WalManager(file_path)
        self._flush_queue = Queue()
        self._do_recoevery()
        self._start_flushing_thread()

    def _do_recoevery(self):
        self._rebuild_sstable_readers()
        self._recover_by_replaying_wal_logs()
        
    def _get_next_id(self):
        sst_files = utils.get_sstfiles(self._file_path)
        if not sst_files:
            return FILE_START_INDEX
        else:
            return utils.get_file_id_from_absolute_path(sst_files[-1]) + 1

    def _start_flushing_thread(self):
        flush_th = Thread(target=self._flushing_thread)
        flush_th.daemon = True
        flush_th.start()

    def _flushing_thread(self):
        try:
            while True:
                store_to_flush, current_wal_path = self._flush_queue.get()
                self._flush_memstore(current_wal_path, store_to_flush)
        except Exception as ex:
            self.logger.debug(
                "Exception {} happened during flushing memstore".format(ex)
            )

    def _flush_memstore(self, wal_path, memstore):
        sst_file_name = self._next_id
        self._next_id += 1
        self.logger.debug(
            "Flushing memstore to file {}".format(
                os.path.join(
                    self._file_path, SST_FILE_NAME_FORMAT.format(sst_file_name)
                )
            )
        )
        sst_writer = SSTableRWMngr(self._file_path, sst_file_name)
        for key, value in memstore.get_all_pairs():
            sst_writer.write_key_value(key, value)
        if wal_path:
            self.logger.debug("Deleting wal file {}".format(wal_path))
            os.remove(wal_path)
        reader = SSTableROMngr(self._file_path, sst_file_name)
        self._sst_mngr.add_reader(reader)

    def _rotate_wal_and_flush_memstore(self):
        current_wal_path = self._wal_mngr.rotate()
        store_to_flush = self._rw_memstore.wo_memstore
        self._rw_memstore.switch_stores()
        self._flush_queue.put((store_to_flush, current_wal_path))

    def _recover_by_replaying_wal_logs(self):
        for key, value in self._wal_mngr.replay():
            self._rw_memstore.put(key, value)
        if self._rw_memstore.size() > 0:
            store_to_flush = self._rw_memstore.wo_memstore
            self._rw_memstore.switch_stores()
            self._flush_memstore(None, store_to_flush)
        utils.remove_all_wal_files(self._file_path)

    def _rebuild_sstable_readers(self):
        sst_files = utils.get_sstfiles(self._file_path)
        for sst_file in sst_files:
            sst_file_name = utils.get_file_id_from_absolute_path(sst_file)
            reader = SSTableROMngr(self._file_path, sst_file_name)
            self._sst_mngr.add_reader(reader)

    def get(self, key):
        with self._lock:
            memstore_val = self._rw_memstore.get(key)
            return_value = ""
            if memstore_val and memstore_val != TOMBSTONE_ENTRY:
                return_value = memstore_val
            elif memstore_val == TOMBSTONE_ENTRY:
                pass
            elif memstore_val is None:
                sstable_value = self._sst_mngr.get_current_reader().get(key)
                if sstable_value is None:
                    pass
                else:
                    return_value = sstable_value
            return return_value

    def put(self, key, value):
        with self._lock:
            self._wal_mngr.append(key, value)
            self._rw_memstore.put(key, value)
            if self._rw_memstore.size() + 1 > self._memstore_max_size:
                self.logger.debug(
                    "Inserted key {} Current size {} of memstore  or equals maximum val {}".format(
                        key, self._rw_memstore.size(), self._memstore_max_size
                    )
                )
                self._rotate_wal_and_flush_memstore()


if __name__ == "__main__":
    db = HelloDB(".", 100)
    
    for i in range(1005):
        key = "test{}".format(i)
        db.put(key, str(i))
    time.sleep(1)
    for i in range(1005):
        value = db.get("test{}".format(i))
        print(value, i)
    time.sleep(10)
