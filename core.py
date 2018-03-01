import errors
import sets


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

    @property
    def is_open(self):
        return True

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

    def subset(self, keys, check_present=True):
        return DataSubset(self, keys, check_present=check_present)

    def map(self, map_fn):
        return MappedDataset(self, map_fn)

    def map_keys(self, key_fn):
        return KeyMappedDataset(self, key_fn)

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

    def is_writable(self):
        return False


class DelegatingDataset(Dataset):
    """
    Minimal wrapping implementation that wraps another dict-like object.

    Wrapped object must have a `keys` and `__getitem__` property. This class
    redirects to those, plus `open` and `close` methods if they exist.

    See also `DictDataset` for a similar implementation that wraps additional
    methods.
    """
    def __init__(self, base_dataset):
        if base_dataset is None:
            raise ValueError('`base_dataset` cannot be None')
        self._base = base_dataset

    @property
    def is_open(self):
        if hasattr(self._base, 'is_open'):
            return self._base.is_open
        else:
            return True

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

    @property
    def is_open(self):
        return True

    def values(self):
        return self._base.values()

    def items(self):
        return self._base.items()


def key_intersection(keys_iterable):
    s = sets.entire_set
    for keys in keys_iterable:
        s = s.intersection(keys)
    return s


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

    @property
    def is_open(self):
        return all(d.is_open for d in self.datasets)

    @property
    def datasets(self):
        return tuple(self._dataset_dict.values())

    def keys(self):
        if self._keys is None:
            self._keys = key_intersection(d.keys() for d in self.datasets)
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

    def save_item(self, key, value):
        if not hasattr(value, 'items'):
            raise TypeError('value must have items for CompoundDataset')
        for value_key, dataset in self._dataset_dict.items():
            dataset.save_item(key, value[value_key])

    def delete_item(self, key):
        for dataset in self.datasets:
            dataset.delete_item(key)


class ZippedDataset(CompoundDataset):
    def __init__(self, *datasets):
        self._datasets = datasets
        self._keys = None

    @property
    def datasets(self):
        return self._datasets

    def __getitem__(self, key):
        return tuple(d[key] for d in self._datasets)

    def save_item(self, key, value):
        if not hasattr(value, '__iter__'):
            raise TypeError('value must be iterable for ZippedDataset')
        for dataset, v in zip(self._datasets, value):
            dataset.save_item(key, v)


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

    def subset(self, keys, check_present=True):
        return self._base.subset(keys, check_present).map(self._map_fn)


class DataSubset(DelegatingDataset):
    """Dataset with keys constrained to a given subset."""
    def __init__(self, base_dataset, keys, check_present=True):
        if base_dataset is None:
            raise ValueError('`base_dataset` cannot be None')
        self._check_present = True
        self._keys = frozenset(keys)
        super(DataSubset, self).__init__(base_dataset)
        if self.is_open:
            self._check_keys()

    def _check_keys(self):
        if self._check_present:
            for key in self._keys:
                if key not in self._base:
                    raise KeyError('key %s not present in base' % key)

    def _with_new_keys(self, keys, check_present):
        return DataSubset(self._base, keys, check_present)

    def subset(self, keys, check_present=True):
        if check_present:
            for key in keys:
                if key not in self._keys:
                    raise KeyError('key %s not present in base' % key)

        return self._with_new_keys(keys, check_present and not self.is_open)

    def keys(self):
        return self._keys

    def __getitem__(self, key):
        if key not in self._keys:
            raise errors.invalid_key_error(self, key)
        return self._base[key]

    def save_item(self, key, value):
        if key in self._keys:
            self._base.save_item(key, value)
        else:
            raise errors.invalid_key_error(self, key)

    def delete_item(self, key):
        if key in self._keys:
            self._base.delete_item(key)
        else:
            raise errors.invalid_key_error(self, key)

    def open(self):
        super(DataSubset, self).open()
        self._check_keys()


class FunctionDataset(Dataset):
    """Dataset which wraps a function."""
    def __init__(self, key_fn):
        self._key_fn = key_fn

    def keys(self):
        raise errors.unknown_keys_error(self)

    def __contains__(self, key):
        return True

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
    def __init__(self, base_dataset, key_fn):
        self._base = base_dataset
        self._key_fn = key_fn

    def keys(self):
        raise errors.unknown_keys_error(self)

    def __len__(self):
        if self._keys is None:
            return len(self._base)
        else:
            return len(self._keys)

    def __getitem__(self, key):
        mapped_key = self._key_fn(key)
        try:
            return self._base[mapped_key]
        except KeyError:
            raise KeyError('%s -> %s not in base dataset ' % (key, mapped_key))

    def __contains__(self, key):
        return self._key_fn(key) in self._base

    def open(self):
        self._base.open()

    def close(self):
        self._base.close()

    def set_item(self, key, value):
        self._base.set_item(self._key_fn(key), value)

    def delete_item(self, key):
        self._base.delete_item(self._key_fn(key))

    @property
    def is_open(self):
        if hasattr(self._base, 'is_open'):
            return self._base.is_open
        else:
            return True
