import json
import logging
from datetime import datetime

from banal import is_listish

from .exceptions import ValidationError
from .util import flatten_dict


log = logging.getLogger(__name__)


def _upsert(db, files, prefix, unique, replace, validate):
    with db as tx:
        if replace:
            # FIXME implement a soft delete? aka backup db file first
            tx["files"].drop()

        # use explicit operations instead of upsert_many to be able to set some
        # more metadata
        existing = [f[unique] for f in db["files"]]
        log.info(f"{len(existing)} exsiting files in `{db}`")
        updated = 0
        added = 0
        invalid = 0

        for file in files:
            try:
                validate(file)
                file[f"{prefix}_last_updated"] = datetime.now()
                if file[unique] in existing:
                    updated += 1
                    tx["files"].update(file, [unique])
                else:
                    file[f"{prefix}_added"] = datetime.now()
                    tx["files"].insert(file)
                    added += 1
            except ValidationError as e:
                invalid += 1
                log.error(f"File `{file[unique]}` not valid: {e}")

    return updated, added, invalid


def load_data(fp, keys):
    # FIXME listish type in sqlite
    with open(fp) as f:
        data = flatten_dict(json.load(f))
    return {
        k: json.dumps(v) if is_listish(v) else v
        for k, v in data.items()
        if not keys or k in keys
    }


def update_state_db(metadir, replace=False):
    """
    update remote metadata to local state
    """

    log.info(f"Updating metadata and state for `{metadir}` ...")

    updated, added, invalid = _upsert(
        metadir._state_db,
        metadir._meta_db["files"],
        "__state",
        metadir.config.unique,
        replace,
        metadir.files.validate,
    )

    metadir.touch("state_last_updated")

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    if invalid:
        log.warning(f"{invalid} invalid files")
    log.info(f'Now {len(metadir._meta_db["files"])} files in state database.')

    return updated, added, invalid


def generate_meta_db(filebackend, metadir, replace=False):
    """
    generate or update file metadata
    """

    log.info(f"Updating metadata for `{filebackend}` ...")

    files = (
        load_data(fp, metadir.config.keys)
        for _, fp in filebackend.get_children(condition=lambda x: x.endswith(".json"))
    )

    updated, added, invalid = _upsert(
        metadir._meta_db,
        files,
        "__meta",
        metadir.config.unique,
        replace,
        metadir.files.validate,
    )

    metadir.touch("meta_last_updated")

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    if invalid:
        log.warning(f"{invalid} invalid files")
    log.info(f'Now {len(metadir._meta_db["files"])} files in meta database.')

    return updated, added, invalid
