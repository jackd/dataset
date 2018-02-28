
def unknown_keys_error(dataset):
    return NotImplementedError(
        'Keys unknown. Specify with `dataset = dataset.subset(keys)` if known')


def invalid_key_error(dataset, key):
    return KeyError('key %s not a valid key' % key)
