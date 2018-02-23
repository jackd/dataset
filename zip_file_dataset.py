import zipfile
import core


class ZipFileDataset(core.Dataset):
    def __init__(self, path):
        self._path = path
        self._file = None

    def open(self):
        if self._file is None:
            self._file = zipfile.ZipFile(self._path)
        else:
            raise IOError('zipfile already open.')

    def close(self):
        if self._file is None:
            raise IOError('zipfile already closed.')
        self._file.close()
        self._file = None
