import os
import json
import core
import save


class JsonDataset(save.SavingDataset, core.DictDataset):
    def __init__(self, path, mode='r'):
        self._path = path
        self._mode = mode
        self._base = None

    @property
    def is_open(self):
        return self._base is not None

    def open(self):
        if self._mode == 'r' and not os.path.isfile(self._path):
            raise IOError(
                'Cannot load json data: file does not exist at %s self._path')
        if self._mode in ('r', 'a') and os.path.isfile(self._path):
            with open(self._path, 'r') as fp:
                self._base = json.load(fp)
        else:
            self._base = {}

    def is_writable(self):
        return self._mode in ('a', 'w')

    def close(self):
        if self.is_writable():
            folder = os.path.dirname(self._path)
            if not os.path.isdir(folder):
                os.makedirs(folder)
            with open(self._path, 'w') as fp:
                json.dump(self._base, fp)
        self._base = None

    def save_item(self, key, value):
        if self.is_writable():
            self._base[key] = value
        else:
            raise IOError('Cannot write to non-writable JsonDataset')

    def delete_item(self, key):
        if self.is_writable():
            del self._base[key]
        else:
            raise IOError('Cannot delete from non-writable JsonDataset')


class JsonAutoSavingManager(save.AutoSavingManager):
    def get_save_path(self, *args, **kwargs):
        raise NotImplementedError('Abstract method')

    def get_saving_dataset(self, *args, **kwargs):
        mode = kwargs.pop('mode', 'a')
        path = self.get_save_path(*args, **kwargs)
        return JsonDataset(path, mode)

# def load_json_dataset(path):
#     if not os.path.isfile(path):
#         raise IOError(
#             'Cannot load json data: file does not exist at %s' % path)
#     with open(path, 'r') as fp:
#         d = json.load(fp)
#     return core.DictDataset(d)
#
#
# def save_json_dataset(path, dataset):
#     try:
#         with open(path, 'w') as fp:
#             json.dump(dataset.to_dict(), fp)
#     except Exception:
#         if os.path.isfile(path):
#             os.remove(path)
#         raise
