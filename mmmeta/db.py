import json
import logging
from datetime import datetime
from itertools import chain
from pathlib import Path

from banal import is_listish

from .exceptions import ValidationError
from .util import checksum, flatten_dict

log = logging.getLogger(__name__)


def _upsert(db, files, prefix, unique, replace, validate, ensure=False):
    def _get_table(tx, name="files"):
        return tx.get_table(name, primary_id=unique, primary_type=tx.types.text)

    with db as tx:
        if replace:
            # FIXME implement a soft delete? aka backup db file first
            tx["files"].drop()

        table = _get_table(tx)
        primary_keys = table.table.primary_key.columns.keys()
        if unique not in primary_keys:
            # table was created before with another primary key
            # this is a bit hacky, but because of SQLite limitations,
            # we just make a new copy of the table with the new primary key...
            tmp_table = _get_table(tx, "tmp")
            tmp_table.insert_many([i for i in table])
            table.drop()
            table = _get_table(tx)
            table.insert_many([i for i in tmp_table])
            tmp_table.drop()

        # use explicit operations instead of upsert_many to be able to set some
        # more metadata
        existing = [f[unique] for f in table]
        log.info(f"{len(existing)} exsiting files in `{db}`")
        updated = 0
        added = 0
        invalid = 0

        for file in files:
            try:
                validate(file)
                file[f"{prefix}_last_updated"] = datetime.now()
                if ensure:
                    file["__mmmeta_last_seen"] = ensure
                if file[unique] in existing:
                    updated += 1
                    table.update(file, [unique])
                else:
                    file[f"{prefix}_added"] = datetime.now()
                    table.insert(file)
                    added += 1
            except ValidationError as e:
                invalid += 1
                log.error(f"File `{file[unique]}` not valid: {e}")

        # soft delete files
        lost_files = []
        if ensure:
            # meta db run
            lost_files = chain(
                table.find(__mmmeta_last_seen={"lt": ensure}),
                table.find(__mmmeta_last_seen=None),
            )
        elif tx.url.endswith("state.db"):
            # state db run
            lost_files = chain(
                table.find(**{unique: {"notin": [f[unique] for f in files]}}),
                files.find(__meta_deleted=1),
            )

        for file in lost_files:
            file[f"{prefix}_deleted_at"] = file.get(
                f"{prefix}_deleted_at", datetime.now()
            )
            file[f"{prefix}_deleted"] = 1
            table.update(file, [unique])

        deleted = len([f for f in table.find(**{f"{prefix}_deleted": 1})])

    return updated, added, invalid, deleted


def _load_metadata(fp, metadir, ensure_files=False):
    keys = metadir.config.keys
    with open(fp) as f:
        data = flatten_dict(json.load(f))
    data = {
        k: json.dumps(v) if is_listish(v) else v  # FIXME listish type in sqlite
        for k, v in data.items()
        if not keys or k in keys
    }
    if ensure_files:
        if not metadir.files.ensure(data):
            data["__meta_deleted"] = 1
            data["__meta_deleted_at"] = datetime.now()
    return data


def _load_files(files):
    for _, fp in files:
        p = Path(fp)
        data = p.stat()
        yield {
            "file_name": p.name,
            "file_path": fp,
            "file_size": data.st_size,
            "created_at": datetime.fromtimestamp(data.st_ctime),
            "modified_at": datetime.fromtimestamp(data.st_mtime),
            "content_hash": checksum(fp),
        }


def update_state_db(metadir, replace=False):
    """
    update remote metadata to local state
    """

    log.info(f"Updating metadata and state for `{metadir}` ...")

    updated, added, invalid, deleted = _upsert(
        metadir._state_db,
        metadir._meta_db["files"],
        "__state",
        metadir.config.unique,
        replace,
        metadir.files.validate,
    )

    metadir.touch("state_last_updated")

    soft_deleted = len(
        [f[metadir.config.unique] for f in metadir.files.find(__meta_deleted=1)]
    )

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    if invalid:
        log.warning(f"{invalid} invalid files")
    if soft_deleted:
        log.warning(f"{soft_deleted} soft deleted files in meta.db")
    if deleted:
        log.warning(f"{deleted} hard deleted files in meta.db")
    log.info(f'Now {len(metadir._meta_db["files"])} files in state database.')

    return updated, added, invalid, deleted


def generate_meta_db(
    filebackend, metadir, replace=False, ensure=False, ensure_files=False, no_meta=False
):
    """
    generate or update file metadata

    ensure: soft delete all previously existing files not found in metadata
    """

    log.info(f"Updating metadata for `{filebackend}` ...")

    if ensure:
        ensure = datetime.now()

    # either read in json metadata files or actual files (only local filesystem here)
    if no_meta:
        files = _load_files(
            filebackend.get_children(condition=lambda x: "_mmmeta" not in x)
        )
    else:
        files = (
            _load_metadata(fp, metadir, ensure_files)
            for _, fp in filebackend.get_children(
                condition=lambda x: x.endswith(".json")
            )
        )

    updated, added, invalid, deleted = _upsert(
        metadir._meta_db,
        files,
        "__meta",
        metadir.config.unique,
        replace,
        metadir.files.validate,
        ensure,
    )

    metadir.touch("meta_last_updated")

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    if invalid:
        log.warning(f"{invalid} invalid files")
    if deleted:
        log.warning(f"{deleted} soft deleted files in meta.db")
    log.info(f'Now {len(metadir._meta_db["files"])} files in meta database.')

    return updated, added, invalid, deleted
