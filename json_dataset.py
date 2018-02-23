import core
import json


def load_json_dataset(path):
    with open(path, 'r') as fp:
        d = json.load(fp)
    return core.DictDataset(d)


def save_json_dataset(path, dataset):
    with open(path, 'w') as fp:
        json.dump(dataset.to_dict(), fp)
