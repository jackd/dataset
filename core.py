class Dataset(object):
    """
    Abstract base class for dict-like interface with convenient wrapping fns.

    Concrete implementations must implement `keys` and `__getitem__`.

    Assumed to be used in a `with` clause, e.g.
    ```
    with MyDataset() as dataset:
        do_stuff_with(dataset)
    ```

    Default `__enter__`/`__exit__` implementations do nothing, but should be
    overriden if accessing files.
    """

    def keys(self):
        raise NotImplementedError('Abstract method')

    def __getitem__(self, key):
        raise NotImplementedError('Abstract method')

    def __contains__(self, key):
        return key in self.keys()

    def __iter__(self):
        return iter(self.keys())

    def values(self):
        return (self[k] for k in self.keys())

    def items(self):
        return ((k, self[k]) for k in self.keys())

    def __len__(self):
        return len(self.keys())

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

    def map_keys(self, key_fn, keys=None):
        return KeyMappedDataset(self, key_fn, keys)

    @staticmethod
    def compound(**datasets):
        return CompoundDataset(**datasets)

    @staticmethod
    def zip(*datasets):
        return ZippedDataset(*datasets)

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
    """
    Minimal wrapping implementation that wraps another dict-like object.

    Wrapped object must have a `keys` and `__getitem__` property. This class
    redirects to those, plus `open` and `close` methods if they exist.

    See also `DictDataset` for a similar implementation that wraps additional
    methods.
    """
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
    """Similar to DelegatingDataset, though redirects more methods."""

    def __contains__(self, key):
        return key in self._base

    def __iter__(self, key):
        return iter(self._base)

    def values(self):
        return self._base.values()

    def items(self):
        return self._base.items()


class CompoundDataset(Dataset):
    """
    A dataset combining a number of datasets.

    The result of combining two datasets with the same keys is a dataset
    with the same keys and values equal to a dictionary of the dataset values.

    e.g.
    ```
    keys = ('hello', 'world')
    x = Dataset.from_function(lambda x: len(x), ('hello', 'world'))
    y = Dataset.from_function(lambda x: x*2, ('world',))
    xy = CompoundDataset(first=x, second=y)
    print(xy.keys())  # 'world'
    print(xy['world'])  # {'first': 5, 'second': 'worldworld'}
    ```
    """
    def __init__(self, **datasets):
        if not all(isinstance(d, Dataset) for d in datasets.values()):
            raise TypeError('All values of `dataset_dict` must be `Dataset`s')
        self._dataset_dict = datasets
        self._keys = None

    def _compute_keys(self):
        datasets = self.datasets
        keys = set(datasets[0].keys())
        for dataset in datasets[1:]:
            keys = keys.intersection(dataset.keys())
        self._keys = frozenset(keys)

    @property
    def datasets(self):
        return tuple(self._dataset_dict.values())

    def keys(self):
        if self._keys is None:
            self._compute_keys()
        return self._keys

    def __getitem__(self, key):
        return {k: v[key] for k, v in self._dataset_dict.items()}

    def __contains__(self, key):
        return all(key in d for d in self.datasets)

    def open(self):
        for v in self.datasets:
            v.open()

    def close(self):
        for v in self.datasets:
            v.close()


class ZippedDataset(CompoundDataset):
    def __init__(self, *datasets):
        self._datasets = datasets
        self._keys = None

    @property
    def datasets(self):
        return self._datasets

    def __getitem__(self, key):
        return tuple(d[key] for d in self._datasets)


class MappedDataset(DelegatingDataset):
    """Dataset representing a mapping applied to a base dataset."""
    def __init__(self, base_dataset, map_fn):
        super(MappedDataset, self).__init__(base_dataset)
        self._map_fn = map_fn

    def __contains__(self, key):
        return key in self._base

    def __getitem__(self, key):
        return self._map_fn(self._base[key])

    def __len__(self):
        return len(self._base)


class DataSubset(DelegatingDataset):
    """Dataset with keys constrained to a given subset."""
    def __init__(self, base_dataset, keys):
        self._keys = frozenset(k for k in keys if k in base_dataset)
        super(DataSubset, self).__init__(base_dataset)

    def keys(self):
        return self._keys

    def __contains__(self, key):
        return key in self._keys

    def __getitem__(self, key):
        if key not in self._keys:
            raise KeyError('key %s not valid key' % key)
        return self._base[key]


class FunctionDataset(Dataset):
    """Dataset which wraps a function."""
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
    """
    Dataset with keys mapped.

    e.g.
    ```
    base = Dataset.from_function(lambda x: len(x), ('hello', 'world'))
    key_mapped = KeyMappedDataset(base, lambda x: x[5:])
    print(key_mapped['ahoy hello'])  # 5
    ```
    """
    def __init__(self, base_dataset, key_fn, keys=None, check_keys=True):
        self._keys = keys
        self._base = base_dataset
        self._key_fn = key_fn
        self._check_keys = check_keys

    def keys(self):
        if self._keys is None:
            raise RuntimeError('keys not supplied in constructor')
        return self._keys

    def __len__(self):
        if self._keys is None:
            return len(self._base)
        else:
            return len(self._keys)

    def __getitem__(self, key):
        try:
            mapped_key = self._key_fn(key)
            return self._base[mapped_key]
        except KeyError:
            raise KeyError('%s -> %s not in base dataset ' % (key, mapped_key))

    def open(self):
        self._base.open()
        if self._check_keys:
            if self._check_keys and self._keys is not None:
                for key in self._keys:
                    mapped_key = self._key_fn(key)
                    if mapped_key not in self._base:
                        raise KeyError('key %s -> %s not in base dataset'
                                       % (key, mapped_key))

    def close(self):
        self._base.close()

    def set_item(self, key, value):
        self._base.set_item(self._key_fn(key), value)

    def delete_item(self, key):
        self._base.delete_item(self._key_fn(key))
