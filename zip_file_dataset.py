import zipfile
import core


class ZipFileDataset(core.Dataset):
    def __init__(self, path, mode='r'):
        self._file = None
        self._path = path
        self._mode = mode
        self._keys = None

    def open(self):
        if self._file is None:
            self._file = zipfile.ZipFile(self._path, self._mode)

    def close(self):
        if self._file is None:
            return
        self._file.close()
        self._file = None

    def keys(self):
        if self._keys is None:
            self._keys = frozenset(self._file.namelist())
        return self._keys

    def __getitem__(self, key):
        return self._file.open(key)
