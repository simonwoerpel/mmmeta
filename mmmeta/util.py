import os
from datetime import datetime
from hashlib import sha1
from pathlib import Path

BUF_SIZE = 1024 * 1024 * 16
HASH_LENGTH = 40  # sha1


def get_files(directory, condition=lambda x: True):
    """
    yield tuples of (filename, path) for files in given `directory`
    that match `condition` (default: all) incl. subdirectories
    """
    return (
        (os.path.splitext(f)[0], os.path.join(d, f))
        for d, _, fnames in os.walk(directory)
        for f in fnames
        if condition(os.path.join(d, f))
    )


def cast(value):
    if not isinstance(value, (str, float, int)):
        return value
    if isinstance(value, str):
        value = value.strip()
    try:
        if float(value) == int(float(value)):
            return int(value)
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value


def flatten_dict(d):
    def items():
        for key, value in d.items():
            key = key.replace("-", "_")
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + ":" + subkey, subvalue
            else:
                yield key, value

    return dict(items())


def ensure_path(file_path):
    if file_path is None or isinstance(file_path, Path):
        return file_path
    return Path(file_path).resolve()


def checksum(file_name):
    """Generate a hash for a given file name."""
    file_name = ensure_path(file_name)
    if file_name is not None and file_name.is_file():
        digest = sha1()
        with open(file_name, "rb") as fh:
            while True:
                block = fh.read(BUF_SIZE)
                if not block:
                    break
                digest.update(block)
        return str(digest.hexdigest())
