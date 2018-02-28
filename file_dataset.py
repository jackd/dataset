import os
import core


class _PathDataset(core.Dataset):
    def __init__(self, root_dir, mode='r'):
        self._root_dir = root_dir
        self._mode = mode

    def is_writable(self):
        return self._mode in ('a', 'w')

    def path(self, key):
        return os.path.join(self._root_dir, key)


class DirectoryDataset(_PathDataset):
    def __contains__(self, key):
        return os.path.exists(self.path(key))

    def __getitem__(self, key):
        path = self.path(key)
        if os.path.isdir(path):
            return DirectoryDataset(path, self._mode)
        else:
            return open(path, self._mode)

    def keys(self):
        n = len(self._root_dir) + 1
        for dirpath, dirnames, filenames in os.walk(self._root_dir):
            dp = dirpath[n:]
            yield dp
            for filename in filenames:
                yield os.path.join(dp, filename)


class FileDataset(_PathDataset):
    def __contains__(self, key):
        return os.path.isfile(self.path(key))

    def __getitem__(self, key):
        return open(self.path(key), self._mode)

    def keys(self):
        n = len(self._root_dir) + 1
        for dirpath, dirnames, filenames in os.walk(self._root_dir):
            dp = dirpath[n:]
            for filename in filenames:
                yield os.path.join(dp, filename)
