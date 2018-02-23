import numpy as np
import h5py
import core


class Hdf5Dataset(core.DictDataset):
    def __init__(self, path):
        self._path = path
        self._base = None

    @property
    def is_open(self):
        return self._base is not None

    def _open(self, mode='r'):
        if self._base is not None:
            raise IOError('Hdf5Dataset already open.')
        self._base = h5py.File(self._path)

    def open(self):
        return self._open()

    def close(self):
        if self._base is not None:
            raise IOError('Hdf5Dataset not open.')
        self._base.close()
        self._base = None

    def _save_np(self, group, key, value, attrs=None):
        assert(isinstance(value, np.ndarray))
        dataset = group.create_dataset(key, data=value)
        if attrs is not None:
            for k, v in attrs.items():
                dataset.attrs[k] = v
        return dataset

    def _save_item(self, group, key, value, overwrite):
        if key in group:
            if overwrite:
                del group[key]
            else:
                return group[key]
        if isinstance(value, np.ndarray):
            return group.create_dataset(key, data=value)
        elif hasattr(value, 'items'):
            subgroup = group.create_group(key)
            for k, v in value.items():
                self._save_item(subgroup, k, v, overwrite)
            return subgroup
        else:
            raise TypeError(
                'value must be numpy array or have `items` attr, got %s'
                % value)

    def _save(self, items, overwrite=False):
        self._open('a')
        for key, value in items:
            self._save_item(self._base, key, value, overwrite)
        self.close()

    def save(self, items, overwrite=False):
        if self.is_open:
            self.close()
            self._save(items, overwrite)
            self.open()
        else:
            self._save(items, overwrite)
