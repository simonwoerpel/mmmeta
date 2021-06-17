import csv
import os
import sys

import dataset
from sqlalchemy.sql import func

from . import settings
from .backend.appendonly import AppendOnlyBackend
from .backend.filesystem import FilesystemBackend
from .backend.store import Store
from .config import Config
from .db import generate_metadata, update_state_db
from .file import FilesWrapper


class Metadir:
    def __init__(self, base_path=None, files_root=None):
        self._base_path = base_path or settings.MMMETA
        self._files_root = files_root or base_path or settings.MMMETA_FILES_ROOT
        self._backend = FilesystemBackend(os.path.join(self._base_path, "_mmmeta"))
        self.config = Config(self)
        self._metadata = AppendOnlyBackend(
            self._backend.get_path("db"), self.config.unique
        )
        self._db_path = f'sqlite:///{self._backend.get_path("state.db")}'
        self.store = Store(FilesystemBackend(self._backend.get_path("_store")))

    def __repr__(self):
        return f"<Metadir: `{self._backend.__class__.__name__}` {self._backend}>"

    def __len__(self):
        return len(self.files)

    @property
    def files(self):
        return FilesWrapper(self._db["files"], self)

    @property  # Shorthand
    def _db(self):
        return dataset.connect(self._db_path)

    def generate(
        self,
        path=None,
        replace=False,
        ensure_metadata=False,
        ensure_files=False,
        no_meta=False,
    ):
        """
        generate or update metadata
        """
        backend = FilesystemBackend(path or self._files_root)
        return generate_metadata(
            backend, self, replace, ensure_metadata, ensure_files, no_meta
        )

    def squash(self):
        self._metadata.squash()

    def update(self, replace=False, cleanup=False):
        """
        update local state with meta db
        """
        return update_state_db(self, replace, cleanup)

    def inspect(self):
        """
        return some insights
        """
        return {
            "files": len(self),
            "path": str(self._backend),
        }

    def touch(self, key):
        """
        store a timestamp with given key
        """
        return self.store.touch(key)

    def dump(self, out=sys.stdout):
        """
        dump csv
        """
        file = self.files.find_one()
        columns = set(file._data.keys()) | set(vars(file.remote).keys())
        writer = csv.DictWriter(out, fieldnames=columns)
        writer.writeheader()
        for file in self.files:
            writer.writerow(file.serialize())

    @property
    def state_last_updated(self):
        table = self.files._table.table
        query = func.max(table.c["__state_last_updated"])
        for res in self._db.query(query):
            return res.get("max_1")

    @property
    def meta_last_updated(self):
        table = self.files._table.table
        query = func.max(table.c["__meta_last_updated"])
        for res in self._db.query(query):
            return res.get("max_1")

    @property
    def last_touched(self):
        return max(
            self.store["store_last_updated"],
            self.state_last_updated,
            self.meta_last_updated,
        )
