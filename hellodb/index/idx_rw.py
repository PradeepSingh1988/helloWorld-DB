import logging
import os
from threading import Lock

from hellodb.consts import (
    CRC_FORMAT,
    IDX_FILE_NAME_FORMAT,
    IDX_HEADER_SIZE,
    IDX_HEADER_FORMAT,
)
from hellodb.io import reader, writer
from hellodb import utils


class IndexWriter(writer.Writer):
    def __init__(self, index_file_path, file_name):
        super().__init__(
            os.path.join(index_file_path, IDX_FILE_NAME_FORMAT.format(file_name)),
            utils.FileEncoder(IDX_HEADER_FORMAT, IDX_HEADER_SIZE, CRC_FORMAT),
        )


class IndexReader(reader.Reader):
    def __init__(self, index_file_path):
        super().__init__(
            index_file_path,
            utils.FileEncoder(IDX_HEADER_FORMAT, IDX_HEADER_SIZE, CRC_FORMAT),
        )
