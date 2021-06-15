import os


def get_env(key, default=None):
    return os.environ.get(key, default)


MMMETA = os.path.abspath(get_env("MMMETA", os.getcwd()))
MMMETA_FILES_ROOT = os.path.abspath(get_env("MMMETA_FILES_ROOT", MMMETA))

LOGGING = get_env("LOGGING")
LOG_FORMAT = get_env("LOG_FORMAT", "TEXT")
