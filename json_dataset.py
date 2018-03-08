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
        if self._mode in ('r', 'a') and os.path.isfile(self._path):
            if os.path.isfile(self._path):
                with open(self._path, 'r') as fp:
                    self._base = json.load(fp)
            else:
                self._base = {}
        else:
            self._base = {}

    def is_writable(self):
        return self._mode in ('a', 'w')

    def close(self):
        if self.is_writable():
            folder = os.path.dirname(self._path)
            if not os.path.isdir(folder):
                os.makedirs(folder)
            try:
                with open(self._path, 'w') as fp:
                    json.dump(self._base, fp)
            except Exception:
                os.remove(self._path)
                raise
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
    def __init__(self, path, saving_message=None):
        self._path = path
        if saving_message is None:
            saving_message = 'Creating data for %s' % path
        self._saving_message = saving_message

    @property
    def saving_message(self):
        return self._saving_message

    @property
    def path(self):
        return self._path

    def get_saving_dataset(self, mode='a'):
        return JsonDataset(self.path, mode)
