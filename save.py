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

    def __delitem__(self, key):
        self.delete_item(key)

    def save_dataset(self, dataset, overwrite=False, show_progress=True,
                     message=None):
        if not self.is_open:
            raise IOError('Cannot save to non-open dataset.')
        keys = dataset.keys()
        if not overwrite:
            keys = [k for k in keys if k not in self]
        if len(keys) == 0:
            return
        if message is not None:
            print(message)
        bar = IncrementalBar(max=len(keys)) if show_progress else DummyBar()
        for key in keys:
            bar.next()
            if key in self:
                self.delete_item(key)
            value = dataset[key]
            self.save_item(key, value)
        bar.finish()

    def save_items(self, items, overwrite=False, show_progress=True):
        if not self.is_open:
            raise IOError('Cannot save to non-open dataset.')
        if show_progress:
            if hasattr(items, '__len__'):
                bar = IncrementalBar(max=len(items))
            else:
                bar = IncrementalBar()
        else:
            bar = DummyBar()
        for key, value in items:
            bar.next()
            if key in self:
                if overwrite:
                    self.delete_item(key)
                else:
                    continue
            self.save_item(key, value)
        bar.finish()

    def subset(self, keys, check_present=True):
        return SavingDataSubset(self, keys, check_present)


class SavingDataSubset(SavingDataset, core.DataSubset):
    def __init__(self, base_dataset, keys, check_present=True):
        if not isinstance(base_dataset, SavingDataset):
            raise TypeError('base_dataset must be a SavingDataset')
        core.DataSubset.__init__(
            self, base_dataset, keys, check_present=check_present)

    def save_item(self, key, value):
        self._base.save_item(key, value)

    def delete_item(self, key):
        self._base.delete_item(key)

    def _with_new_keys(self, keys, check_present):
        return SavingDataSubset(self._base, keys, check_present)


class AutoSavingDataset(core.Dataset):
    def __init__(self, src, dst):
        if not isinstance(dst, SavingDataset):
            raise TypeError('`dst` must be a `SavingDataset`')
        if not all(hasattr(src, k) for k in ('items', '__getitem__')):
            raise TypeError('`src` must have `items` and `__getitem__` attrs')
        self._src = src
        self._dst = dst

    @property
    def src(self):
        """The source dataset this dataset gets data from."""
        return self._src

    @property
    def dst(self):
        """The destination dataset this dataset saves to."""
        return self._dst

    def unsaved_keys(self):
        return (k for k in self.src.keys() if k not in self.dst)

    def keys(self):
        return self._src.keys()

    def __contains__(self, key):
        return key in self._src

    def __getitem__(self, key):
        if key in self._dst:
            return self._dst[key]
        else:
            value = self._src[key]
            self._dst.save_item(key, value)
            return value

    def save_all(self, overwrite=False, show_progress=True, message=None):
        self.dst.save_dataset(
            self.src, overwrite=overwrite, show_progress=show_progress,
            message=message)

    def open(self):
        self.src.open()
        self.dst.open()

    def close(self):
        self.dst.close()
        self.src.close()

    def subset(self, keys, check_present=True):
        src = self.src.subset(keys, check_present)
        # dst = self.dst.subset(keys, False)
        dst = self.dst
        return AutoSavingDataset(src, dst)


def get_auto_saving_dataset_fn(lazy_fn, saving_fn):
    def auto_saving_fn(*args, **kwargs):
        src = lazy_fn(*args, **kwargs)
        dst = saving_fn(*args, mode='a', **kwargs)
        return AutoSavingDataset(src, dst)

    return auto_saving_fn


class AutoSavingManager(object):
    def get_lazy_dataset(self):
        raise NotImplementedError('Abstract method')

    def get_saving_dataset(self, mode='r'):
        raise NotImplementedError('Abstract method')

    def get_auto_saving_dataset(self, mode='a'):
        # lazy = self.get_lazy_dataset()
        # active = self.get_saving_dataset(mode)
        # exit()
        return AutoSavingDataset(
            self.get_lazy_dataset(),
            self.get_saving_dataset(mode))

    @property
    def saving_message(self):
        return None

    def save_all(self, overwrite=False):
        with self.get_auto_saving_dataset('a') as ds:
            ds.save_all(overwrite=overwrite, message=self.saving_message)

    def get_saved_dataset(self):
        self.save_all()
        return self.get_saving_dataset(mode='r')

# class AutoSavingManager(object):
#
#     def get_lazy_dataset(self, *args, **kwargs):
#         raise NotImplementedError('Abstract method')
#
#     def get_saving_dataset(self, *args, **kwargs):
#         raise NotImplementedError('Abstract method')
#
#     def get_auto_saving_dataset(self, *args, **kwargs):
#         mode = kwargs.pop('mode', 'a')
#         src = self.get_lazy_dataset(*args, **kwargs)
#         kwargs['mode'] = mode
#         dst = self.get_saving_dataset(*args, **kwargs)
#         return AutoSavingDataset(src, dst)
#
#     def save_all(self, *args, **kwargs):
#         overwrite = kwargs.pop('overwrite', False)
#         kwargs['mode'] = 'a'
#         message = kwargs.pop('message', None)
#         with self.get_auto_saving_dataset(*args, **kwargs) as ds:
#             ds.save_all(overwrite=overwrite, message=message)
