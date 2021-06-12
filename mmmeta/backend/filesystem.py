import os
import shutil

from ..util import get_files
from .base import Backend


def ensure_directory(fp):
    fp = os.path.abspath(fp)
    if not os.path.isdir(fp):
        os.makedirs(fp)
    return fp


class FilesystemBackend(Backend):
    def get_base_path(self):
        return ensure_directory(self.data_root)

    def exists(self, path):
        p = self.get_path(path)
        return any((os.path.isfile(p), os.path.isdir(p)))

    def save(self, path, content):
        path = self.get_path(path)
        ensure_directory(os.path.split(path)[0])
        with open(path, "w") as f:
            content = str(content)
            f.write(content)
        return path

    def _load(self, path):
        path = self.get_path(path)
        with open(path) as f:
            content = f.read().strip()
        return content

    def get_children(self, path=".", condition=lambda x: True):
        path = self.get_path(path)
        return get_files(path, condition)

    def delete(self, path=""):
        path = self.get_path(path)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
