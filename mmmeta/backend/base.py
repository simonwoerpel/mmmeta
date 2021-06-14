import json
import os

from ..util import cast, datetime_to_json


class Backend:
    """
    base class for metadir backends.
    currently only local filesystem backend implemented.
    """

    def __init__(self, data_root):
        self.data_root = data_root
        self.base_path = self.get_base_path()

    def __str__(self):
        return self.get_base_path()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self}>"

    def get_base_path(self):
        """return a base path to a local file dir or a cloud bucket"""
        raise NotImplementedError

    def get_path(self, path):
        """return absolute filesystem path or cloud bucket for `path"""
        return os.path.join(self.base_path, path)

    def exists(self, path):
        """check if given path exists and return boolean"""
        raise NotImplementedError

    def save(self, path, content):
        """
        store `content` in path and return absolute path to stored file or
        cloud blob location
        """
        raise NotImplementedError

    def load(self, path):
        """
        return content as string for given path, use the same not found
        exception for all storages:
        """
        if not self.exists(path):
            raise FileNotFoundError(f"Path `{path}` not found in storage `{self}`")
        return self._load(path)

    def load_json(self, path):
        return json.loads(self.load(path))

    def dump_json(self, path, content):
        content = json.dumps(content, default=datetime_to_json)
        self.save(path, content)

    def _load(self, path):
        """actual implementation for specific storage"""
        raise NotImplementedError

    def set_value(self, path, value):
        """simply store values to a path location"""
        self.save(path, value)
        return value

    def get_value(self, path, transform=lambda x: cast(x, with_date=True)):
        """simply get values from a path location"""
        if not self.exists(path):
            return
        content = self.load(path)
        return transform(content)

    def get_children(self, path=".", condition=lambda x: True):
        """list all children under given path that match condition"""
        raise NotImplementedError

    def delete(self, path=""):
        """delete everything from path"""
        raise NotImplementedError
