import os
import numpy as np
import save


class NumpyDataset(save.SavingDataset):
    def __init__(self, base_dir):
        self._base_dir = base_dir

    def keys(self):
        return (k[:-4] for k in os.listdir(self._base_dir))

    def _path(self, key):
        return os.path.join(self._base_dir, '%s.npy' % key)

    def __getitem__(self, key):
        return np.load(self._path(key))

    def __contains__(self, key):
        return os.path.isfile(self._path(key))

    def save_item(self, key, value):
        if not isinstance(value, np.ndarray):
            raise TypeError('value must be a numpy array.')
        np.save(self._path(key), value)

    def delete_item(self, key):
        os.remove(self._path(key))
