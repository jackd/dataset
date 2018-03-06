import core


class ParentDataset(core.DelegatingDataset):
    def __init__(self, base_dataset):
        self._open_children = set()
        core.DelegatingDataset.__init__(self, base_dataset)

    def _on_child_opened(self, child):
        if child in self._open_children:
            raise KeyError('Already open %s' % child)
        self._open_children.add(child)
        if not self.is_open:
            self.open()

    def _on_child_closed(self, child):
        if child not in self._open_children:
            raise KeyError('key %s not present' % child)
        self._open_children.remove(child)
        if len(self._open_children) == 0:
            self.close()


class ChildDataset(core.DelegatingDataset):
    def __init__(self, parent, base_dataset):
        self._parent = parent
        super(ChildDataset, self).__init__(base_dataset)

    def is_open(self):
        return self in self._parent._open_children

    def open(self):
        self._parent._on_child_opened(self)

    def close(self):
        self._parent._on_child_closed(self)
