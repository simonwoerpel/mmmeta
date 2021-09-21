import os
from datetime import date, datetime
from hashlib import sha1
from pathlib import Path

# from banal import as_bool, clean_dict

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


def cast(value, with_date=False):
    if not isinstance(value, (str, float, int)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:  # ''
            return None
    try:
        if float(value) == int(float(value)):
            return int(value)
        return float(value)
    except (TypeError, ValueError):
        if with_date:
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        # value = as_bool(value, None)
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


def casted_dict(d, ignore_keys=[]):
    return {
        k: cast(v, with_date=True) if k not in ignore_keys else v for k, v in d.items()
    }


def robust_dict(d):
    # no typing for better comparison performance
    return {k: str(v) if v else None for k, v in flatten_dict(d).items()}


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


def dict_diff(dict1, dict2):
    """
    return key/value pairs from dict1 that are different from dict2
    """
    return set(flatten_dict(dict1).items()) - set(flatten_dict(dict2).items())


def dict_is_subset(dict1, dict2, ignore=set()):
    """
    check if dict1 is contained in dict2 (including values)
    """
    return len(dict_diff(dict1, dict2) - ignore) == 0


def datetime_to_json(value):
    if isinstance(value, date):
        return value.isoformat()
