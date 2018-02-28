import os
import numpy as np
import h5py
import core
import save


class Hdf5Dataset(core.DictDataset, save.SavingDataset):
    def __init__(self, path, mode='r'):
        self._path = path
        self._mode = mode
        self._base = None

    @property
    def path(self):
        return self._path

    @property
    def is_open(self):
        return self._base is not None

    def open(self):
        if self._base is not None:
            raise IOError('Hdf5Dataset already open.')
        if self._mode == 'r' and not os.path.isfile(self._path):
            raise IOError('No file at %s' % self._path)
        self._base = h5py.File(self._path, self._mode)

    def is_writable(self):
        return self._mode in ('a', 'w')

    def close(self):
        if self._base is not None:
            self._base.close()
            self._base = None

    def _save_np(self, group, key, value, attrs=None):
        assert(isinstance(value, np.ndarray))
        dataset = group.create_dataset(key, data=value)
        if attrs is not None:
            for k, v in attrs.items():
                dataset.attrs[k] = v
        return dataset

    def _save_item(self, group, key, value):
        if isinstance(value, np.ndarray):
            return group.create_dataset(key, data=value)
        elif key == 'attrs':
            if not hasattr(value, 'items'):
                raise ValueError('attrs value must have `items` attr')
            for k, v in value.items():
                group.attrs[k] = v
        elif hasattr(value, 'items'):
            try:
                subgroup = group.create_group(key)
                for k, v in value.items():
                    self._save_item(subgroup, k, v)
                return subgroup
            except Exception:
                if key in subgroup:
                    del subgroup[key]
                raise
        else:
            raise TypeError(
                'value must be numpy array or have `items` attr, got %s'
                % str(value))

    def save_item(self, key, value):
        self._save_item(self._base, key, value)

    def delete_item(self, key):
        del self._base[key]


class Hdf5AutoSavingManager(save.AutoSavingManager):
    def get_save_path(self, *args, **kwargs):
        raise NotImplementedError('Abstract method')

    def get_saving_dataset(self, *args, **kwargs):
        mode = kwargs.pop('mode', 'r')
        path = self.get_save_path(*args, **kwargs)
        return Hdf5Dataset(path, mode)
