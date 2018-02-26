import core
from progress.bar import IncrementalBar


class DummyBar(object):
    def next(self):
        pass

    def finish():
        pass


class SavingDataset(core.Dataset):
    def save_item(self, key, value):
        raise NotImplementedError('Abstract method')

    def delete_item(self, key):
        raise NotImplementedError('Abstract method')

    def __setitem__(self, key, value):
        self.save_item(key, value)

    def __del__(self, key):
        self.delete_item(key)

    def save_dataset(self, dataset, overwrite=False, show_progress=True):
        if not self.is_open:
            raise IOError('Cannot save to non-open dataset.')
        bar = IncrementalBar(max=len(dataset)) if show_progress else DummyBar()
        for key in dataset:
            bar.next()
            if key in self:
                if overwrite:
                    self.delete_item(key)
                else:
                    continue
            value = dataset[key]
            self.save_item(key, value)
        bar.finish()

    def save_items(self, items, overwrite=False, show_progress=True):
        if not self.is_open:
            raise IOError('Cannot save to non-open dataset.')
        bar = IncrementalBar(max=len(items)) if show_progress else DummyBar()
        for key, value in items:
            bar.next()
            if key in self:
                if overwrite:
                    self.delete_item(key)
                else:
                    continue
            self.save_item(key, value)
        bar.finish()


class AutoSavingDataset(core.Dataset):
    def __init__(self, src, dst):
        if not isinstance(dst, SavingDataset):
            raise TypeError('`dst` must be a `SavingDataset`')
        if not all(hasattr(src, k) for k in ('items', '__getitem__')):
            raise TypeError('`src` must have `items` and `__getitem__` attrs')
        self._src = src
        self._dst = dst

    def keys(self):
        return self._src.keys()

    def __getitem__(self, key):
        if key in self._dst:
            return self._dst[key]
        else:
            value = self._src[key]
            self._dst.save_item(key, value)
            return value

    def save_all(self, overwrite=False, show_progress=True):
        self._dst.save_dataset(
            self._src, overwrite=overwrite, show_progress=show_progress)

    def open(self):
        self._src.open()
        self._dst.open()

    def close(self):
        self._src.close()
        self._dst.close()


def get_auto_saving_dataset_fn(lazy_fn, saving_fn):
    def auto_saving_fn(*args, **kwargs):
        src = lazy_fn(*args, **kwargs)
        dst = saving_fn(*args, mode='a', **kwargs)
        return AutoSavingDataset(src, dst)
    return auto_saving_fn


class AutoSavingManager(object):

    def get_lazy_dataset(self, *args, **kwargs):
        raise NotImplementedError('Abstract method')

    def get_saving_dataset(self, *args, **kwargs):
        raise NotImplementedError('Abstract method')

    def get_auto_saving_dataset(self, *args, **kwargs):
        mode = kwargs.pop('mode', 'a')
        src = self.get_lazy_dataset(*args, **kwargs)
        kwargs['mode'] = mode
        dst = self.get_saving_dataset(*args, **kwargs)
        return AutoSavingDataset(src, dst)

    def save_all(self, *args, **kwargs):
        overwrite = kwargs.pop('overwrite', False)
        kwargs['mode'] = 'a'
        with self.get_auto_saving_dataset(*args, **kwargs) as ds:
            ds.save_all(overwrite=overwrite)
