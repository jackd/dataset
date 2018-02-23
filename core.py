class Dataset(object):
    def keys(self):
        raise NotImplementedError('Abstract method')

    def __getitem__(self, key):
        raise NotImplementedError('Abstract method')

    def __contains__(self, key):
        return key in self.keys()

    def values(self):
        return (self[k] for k in self.keys())

    def items(self):
        return ((k, self[k]) for k in self.keys())

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def open(self):
        pass

    def close(self):
        pass

    def to_dict(self):
        return {k: v for k, v in self.items()}

    def subset(self, keys):
        return DataSubset(self, keys)

    def map(self, map_fn):
        return MappedDataset(self, map_fn)

    @staticmethod
    def compound(**datasets):
        return CompoundDataset(**datasets)

    @staticmethod
    def from_dict(dictionary):
        return DictDataset(dictionary)

    @staticmethod
    def wrapper(dictish):
        return DelegatingDataset(dictish)

    @staticmethod
    def from_function(key_fn, keys):
        return FunctionDataset(key_fn, keys)


class DelegatingDataset(Dataset):
    def __init__(self, base_dataset):
        self._base = base_dataset

    def keys(self):
        return self._base.keys()

    def __getitem__(self, key):
        return self._base[key]

    def open(self):
        if hasattr(self._base, 'open'):
            self._base.open()

    def close(self):
        if hasattr(self._base, 'close'):
            self._base.close()


class DictDataset(DelegatingDataset):

    def __contains__(self, key):
        return key in self._base

    def values(self):
        return self._base.values()

    def items(self):
        return self._base.items()


class CompoundDataset(Dataset):
    def __init__(self, **datasets):
        if not all(isinstance(d, Dataset) for d in datasets.values()):
            raise TypeError('All values of `dataset_dict` must be `Dataset`s')
        self._dataset_dict = datasets

    def keys(self):
        datasets = tuple(self._dataset_dict.values())
        keys = datasets[0].keys()
        datasets = datasets[1:]
        for k in keys:
            if all(k in d for d in datasets):
                yield k

    def __getitem__(self, key):
        return {k: v[key] for k, v in self._dataset_dict.items()}

    def __contains__(self, key):
        return all(key in d for d in self._dataset_dict.values())

    def open(self):
        for v in self._dataset_dict.values():
            v.open()

    def close(self):
        for v in self._dataset_dict.values():
            v.close()


class MappedDataset(DelegatingDataset):
    def __init__(self, base_dataset, map_fn):
        super(MappedDataset, self).__init__(base_dataset)
        self._map_fn = map_fn

    def __contains__(self, key):
        return key in self._base

    def __getitem__(self, key):
        return self._map_fn(super(MappedDataset, self)[key])


class DataSubset(DelegatingDataset):
    def __init__(self, base_dataset, keys):
        if not all(k in base_dataset for k in keys):
            raise ValueError('Not all keys in base dataset.')
        self._keys = keys
        super(DataSubset, self).__init__(base_dataset)

    def keys(self):
        return self._keys

    def __contains__(self, key):
        return key in self._keys

    def __getitem__(self, key):
        if key not in self._keys:
            raise KeyError('key %s not valid key' % key)
        return super(DataSubset, self)[key]


class FunctionDataset(Dataset):
    def __init__(self, key_fn, keys):
        self._keys = keys
        self._key_fn = key_fn

    def keys(self):
        return self._keys

    def __contains__(self, key):
        return key in self._keys

    def __getitem__(self, key):
        return self._key_fn(key)


class KeyMappedDataset(Dataset):
    def __init__(self, base_dataset, keys, key_fn):
        for key in keys:
            base_key = key_fn(key)
            if base_key not in base_dataset:
                raise KeyError(
                    'Invalid key/key_fn/dataset combination, %s -> %s' %
                    (key, base_key))
        self._base = base_dataset
        self._key_fn = key_fn
        self._keys = keys

    def keys(self):
        return self._keys

    def __getitem__(self, key):
        return self._base[self._key_fn(key)]

    def open(self):
        self._base.open()

    def close(self):
        self._base.close()
