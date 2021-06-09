import os

import dataset
from sqlalchemy.sql import func

from . import settings
from .backend.filesystem import FilesystemBackend
from .backend.store import Store
from .config import Config
from .db import generate_meta_db, update_state_db
from .file import FilesWrapper


class Metadir:
    def __init__(self, base_path=None, files_root=None):
        self._base_path = base_path or settings.MMMETA
        self._files_root = files_root or base_path or settings.MMMETA_FILES_ROOT
        self._backend = FilesystemBackend(os.path.join(self._base_path, "_mmmeta"))
        self._meta_db_path = f'sqlite:///{self._backend.get_path("meta.db")}'
        self._state_db_path = f'sqlite:///{self._backend.get_path("state.db")}'
        self.config = Config(self)
        self.store = Store(FilesystemBackend(self._backend.get_path("_store")))

    def __repr__(self):
        return f"<Metadir: `{self._backend.__class__.__name__}` {self._backend}>"

    def __len__(self):
        return len(self.files)

    @property
    def files(self):
        return FilesWrapper(self._state_db["files"], self)

    @property
    def _meta_db(self):
        return dataset.connect(self._meta_db_path)

    @property
    def _state_db(self):
        return dataset.connect(self._state_db_path)

    @property  # Shorthand
    def _db(self):
        return self._state_db

    def generate(
        self, path=None, replace=False, ensure=False, ensure_files=False, no_meta=False
    ):
        """
        generate or update meta db
        """
        backend = FilesystemBackend(path or self._files_root)
        return generate_meta_db(backend, self, replace, ensure, ensure_files, no_meta)

    def update(self, replace=False):
        """
        update local state with meta db
        """
        return update_state_db(self, replace)

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

    @property
    def state_last_updated(self):
        table = self.files._table.table
        query = func.max(table.c["__state_last_updated"])
        for res in self._state_db.query(query):
            return res.get("max_1")

    @property
    def meta_last_updated(self):
        table = self.files._table.table
        query = func.max(table.c["__meta_last_updated"])
        for res in self._state_db.query(query):
            return res.get("max_1")

    @property
    def last_touched(self):
        return max(
            self.store["store_last_updated"],
            self.state_last_updated,
            self.meta_last_updated,
        )
