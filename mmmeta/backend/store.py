from datetime import datetime

from ..exceptions import StoreError


class Store:
    """
    simple, file-system based key-value store
    """

    def __init__(self, backend):
        self._backend = backend

    def __str__(self):
        return str(self._backend)

    def __repr__(self):
        return repr(self._backend)

    def __getitem__(self, attr):
        if "/" in attr:
            raise StoreError(f"illegal key: {attr}")
        return self._backend.get_value(attr)

    def __setitem__(self, attr, value=""):
        if "/" in attr:
            raise StoreError(f"illegal key: {attr}")
        self._backend.set_value(attr, value)
        self.touch()

    def __iter__(self):
        for key, _ in self._backend.get_children():
            yield key, self[key]

    def touch(self, key="store_last_updated"):
        self._backend.set_value(key, datetime.now().isoformat())
        if key != "store_last_updated":
            # of course we like to touch always everything
            self._backend.set_value("store_last_updated", datetime.now().isoformat())

    def serialize(self):
        return {k: v for k, v in self}

    def to_string(self):
        tmpl = "{k}: {v}"
        return "\n".join(tmpl.format(k=k, v=v) for k, v in self)
