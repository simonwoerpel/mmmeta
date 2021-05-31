import os


def get_env(key, default=None):
    return os.environ.get(key, default)


MMMETA = get_env("MMMETA", os.getcwd())
MMMETA_FILES_ROOT = get_env("MMMETA_FILES_ROOT", MMMETA)
