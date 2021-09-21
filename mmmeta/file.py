import os
from datetime import datetime
from types import SimpleNamespace

from banal import clean_dict

from .exceptions import ValidationError


class File:
    def __init__(self, metadir, data):
        self._metadir = metadir
        self._data = data
        self._unique = metadir.config.unique

    def __setitem__(self, attr, value):
        self.update(**{attr: value})

    def __getitem__(self, attr, default=None):
        return self._data.get(attr, default)

    def __contains__(self, key):
        return key in self._data

    def update(self, **data):
        """
        bulk attribute update (like dict.update)
        update internal data object and write to state db
        """
        self._data.update(**data)
        self._data["__state_last_updated"] = datetime.now()

    def save(self):
        with self._metadir._db as db:
            db["files"].update(self._data, [self._unique])

    def serialize(self):
        return {**self._data, **vars(self.remote)}

    @property
    def uid(self):
        return self._data[self._unique]

    @property
    def name(self):
        return self._data.get(self._metadir.config.file_name, self.uid)

    @property
    def remote(self):
        return SimpleNamespace(**dict(self._metadir.config.get_remote(self._data)))


class FilesWrapper:
    """
    yield actual `File` objects from dataset table,
    pass other operations through `dataset.table.Table`

    https://dataset.readthedocs.io/en/latest/quickstart.html#reading-data-from-tables
    """

    def __init__(self, table, metadir):
        self._table = table
        self._metadir = metadir
        self.config = metadir.config

    def __iter__(self):
        for data in self._table:
            yield File(self._metadir, data)

    def __len__(self):
        return len(self._table)

    def __contains__(self, file):
        return bool(self.find_one(**{self.config.unique: file[self.config.unique]}))

    def find(self, *args, **kwargs):
        for data in self._table.find(*args, **kwargs):
            yield File(self._metadir, data)

    def find_one(self, *args, **kwargs):
        data = self._table.find_one(*args, **kwargs)
        if data:
            return File(self._metadir, data)

    def __getattr__(self, attr):
        """
        pass through dataset table funcionality
        """
        return getattr(self._table, attr)

    def validate(self, data):
        """
        check if data dict has all required keys from config
        """
        if hasattr(data, "_data"):
            data = data._data
        data = clean_dict(data)
        remaining = self.config.required_keys - set(data.keys())
        if remaining:
            raise ValidationError(f"Missing keys: {remaining}")
        return True

    def ensure(self, data):
        """
        ensure that an actual file (local storage only) really exists
        """
        if hasattr(data, "_data"):
            data = data._data
        data = clean_dict(data)
        fp = os.path.join(
            self._metadir._files_root, data[self._metadir.config.file_name]
        )
        return os.path.exists(fp)
