import os

from datetime import datetime


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
