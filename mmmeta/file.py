from datetime import datetime


class File:
    def __init__(self, metadir, data, identifier="content_hash"):
        self._metadir = metadir
        self._data = data
        self._identifier = identifier
        self.id = self._data[identifier]

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
        with self._metadir._state_db as db:
            db["files"].update(self._data, [self._identifier])
        self._metadir.touch("state_last_updated")


class FilesWrapper:
    """
    yield actual `File` objects from dataset table,
    pass other operations through `dataset.table.Table`

    https://dataset.readthedocs.io/en/latest/quickstart.html#reading-data-from-tables
    """

    def __init__(self, table, metadir):
        self._table = table
        self._metadir = metadir

    def __iter__(self):
        for data in self._table:
            yield File(self._metadir, data)

    def __len__(self):
        return len(self._table)

    def __contains__(self, file):
        return bool(self.find_one(id=file["id"]))

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
