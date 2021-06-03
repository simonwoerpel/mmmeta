import os

import dataset

from .backend.filesystem import FilesystemBackend
from .backend.store import Store
from .db import generate_meta_db, update_state_db
from .file import FilesWrapper
from . import settings


class Metadir:
    def __init__(self, base_path=None):
        self._base_path = base_path or settings.MMMETA
        self._backend = FilesystemBackend(os.path.join(self._base_path, "_mmmeta"))
        self._meta_db_path = f'sqlite:///{self._backend.get_path("meta.db")}'
        self._state_db_path = f'sqlite:///{self._backend.get_path("state.db")}'
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

    def generate(self, path=None, replace=False, unique="content_hash"):
        """
        generate or update meta db
        """
        path = path or self._base_path
        backend = FilesystemBackend(path)
        generate_meta_db(backend, self, replace)

    def update(self, unique="content_hash"):
        """
        update local state with meta db
        """
        update_state_db(self)

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
    def state_last_touched(self):
        return self.store["state_last_updated"]

    @property
    def meta_last_touched(self):
        return self.store["meta_last_updated"]

    @property
    def last_touched(self):
        return self.store["store_last_updated"]
