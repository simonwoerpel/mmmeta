import json
import logging
from datetime import datetime

from banal import is_listish


log = logging.getLogger(__name__)


def _upsert(db, files, prefix, unique="content_hash"):
    with db as tx:
        # use explicit operations instead of upsert_many to be able to set some
        # more metadata
        existing = [f[unique] for f in db["files"]]
        log.info(f"{len(existing)} exsiting files in `{db}`")
        updated = 0
        added = 0

        for file in files:
            file[f"{prefix}_last_updated"] = datetime.now()
            if file[unique] in existing:
                updated += 1
                tx["files"].update(file, [unique])
            else:
                file[f"{prefix}_added"] = datetime.now()
                tx["files"].insert(file)
                added += 1
    return updated, added


def to_db_json(data):
    # FIXME
    return {k: json.dumps(v) if is_listish(v) else v for k, v in data.items()}


def update_state_db(metadir, unique="content_hash"):
    """
    update remote metadata to local state
    """

    log.info(f"Updating metadata and state for `{metadir}` ...")

    updated, added = _upsert(
        metadir._state_db, metadir._meta_db["files"], "__state", unique
    )

    metadir.touch("state_last_updated")

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    log.info(f'Now {len(metadir._meta_db["files"])} files in state database.')


def generate_meta_db(filebackend, metadir, replace=False, unique="content_hash"):
    """
    generate or update file metadata
    """

    log.info(f"Updating metadata for `{filebackend}` ...")

    if replace:
        # soft delete current meta.db
        pass

    files = (
        to_db_json(json.load(open(fp)))
        for _, fp in filebackend.get_children(condition=lambda x: x.endswith(".json"))
    )

    updated, added = _upsert(metadir._meta_db, files, "__meta", unique)

    metadir.touch("meta_last_updated")

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    log.info(f'Now {len(metadir._meta_db["files"])} files in meta database.')
