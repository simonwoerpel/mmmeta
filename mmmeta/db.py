import logging
from datetime import datetime
from itertools import chain
from pathlib import Path

import dataset
from banal import ensure_dict
from sqlalchemy.exc import IntegrityError

from .exceptions import ValidationError
from .util import casted_dict, checksum, dict_diff, dict_is_subset, robust_dict

log = logging.getLogger(__name__)


def _upsert(tx, metadir, files, prefix, ts, ensure=False, casted=False):
    # use explicit operations instead of upsert_many to be able to set some
    # more metadata
    table = tx["files"]
    to_insert = []
    updated = added = invalid = deleted = skipped = 0
    ignore_seen = set((("__seen", ts),))
    for file in files:
        if casted:
            file = casted_dict(file)
        if ensure:
            file["__seen"] = ts  # helper to do a quick scan later
        try:
            metadir.files.validate(file)
            uid = file[metadir.config.unique]
            existing_file = table.find_one(**{metadir.config.unique: uid})
            if existing_file:
                if dict_is_subset(file, existing_file, ignore_seen):
                    skipped += 1
                else:
                    if file.get("__deleted", None) is not None:
                        deleted += 1
                    else:
                        file[f"__{prefix}_last_updated"] = ts
                        updated += 1
                table.upsert(file, [metadir.config.unique])
            else:
                file[f"__{prefix}_added"] = ts
                file[f"__{prefix}_last_updated"] = ts
                to_insert.append(file)
                added += 1
        except ValidationError as e:
            invalid += 1
            fname = file.get(metadir.config.unique) or "undefined"
            log.error(f"File `{fname}` not valid: {e}")

    # at least bulk insert
    table.insert_many(to_insert)

    if ensure:
        for file in chain(
            table.find(__seen={"lt": ts}),
            table.find(__seen=None),
        ):
            file["__deleted"] = 1
            file["__deleted_at"] = ts
            file["__deleted_reason"] = f"{prefix}-missing"
            file[f"__{prefix}_last_updated"] = ts
            table.update(file, [metadir.config.unique])
            deleted += 1

    new_count = len(table)

    log.info(f"Updated {updated} files.")
    log.info(f"Added {added} new files.")
    log.info(f"Skipped {skipped} not changed files.")
    if invalid:
        log.warning(f"{invalid} invalid files")
    if deleted:
        log.warning(f"{deleted} soft deleted files")
    log.info(f"Now {new_count} files in database.")

    return updated, added, invalid, deleted, skipped


def _load_metadata(fp, metadir, ts, ensure_files=False):
    # use robust dict for performance
    keys = metadir.config.keys
    data = metadir._backend.load_json(fp)
    data = robust_dict(data)
    data = {k: v for k, v in data.items() if not keys or k in keys}
    if ensure_files:
        if not metadir.files.ensure(data):
            data["__meta_last_updated"] = ts
            data["__deleted"] = 1
            data["__deleted_at"] = ts
            data["__deleted_reason"] = "original-missing"
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


def _get_table(tx, primary_id, name="files"):
    return tx.get_table(name, primary_id=primary_id, primary_type=tx.types.text)


def update_state_db(metadir, replace=False, cleanup=False):
    """
    update remote metadata to local state
    """

    log.info(f"Updating metadata and state for `{metadir}` ...")

    with metadir._db as tx:
        if replace:
            # FIXME implement a soft delete? aka backup db file first
            tx["files"].drop()

        table = _get_table(tx, metadir.config.unique)
        primary_keys = table.table.primary_key.columns.keys()
        if metadir.config.unique not in primary_keys:
            # table was created before with another primary key
            # this is a bit hacky, but because of SQLite limitations,
            # we just make a new copy of the table with the new primary key...
            tmp_table = _get_table(tx, metadir.config.unique, "tmp")
            try:
                tmp_table.insert_many([i for i in table])
            except IntegrityError as e:
                log.warning(
                    f"Cannot perform `state.db` migration under such circumstances. Is your config correct? `{e}`"  # noqa
                )
            table.drop()
            table = _get_table(tx, metadir.config.unique)
            table.insert_many([i for i in tmp_table])
            tmp_table.drop()

        if cleanup:
            log.info("Cleaning up ...")
            tmp_table = _get_table(tx, metadir.config.unique, "tmp")
            keys = metadir.config.keys
            tmp_table.insert_many(
                [
                    casted_dict(
                        {
                            k: v
                            for k, v in f.items()
                            if not keys or k in keys or k.startswith("_")
                        }
                    )
                    for f in table
                ]
            )
            table = table.drop()
            table = _get_table(tx, metadir.config.unique)
            table.insert_many([i for i in tmp_table])

        log.info(f"{len(table)} exsiting files in `{tx}`")

        files = tx["meta_files"]
        metadir._metadata.load(files)
        # use a consistent timestamp for state diff queries
        ts = datetime.now()
        res = _upsert(tx, metadir, files, "state", ts, ensure=True, casted=True)
        files.drop()

    if any(res[:4]):
        # added or updated:
        metadir.touch("state_last_updated")
    return res


def generate_metadata(
    filebackend,
    metadir,
    replace=False,
    ensure_metadata=False,
    ensure_files=False,
    no_meta=False,
):
    """
    generate or update file metadata

    it is stored under `_mmmeta/db/*.json.meta` one file for each file

    ensure: soft delete all previously existing files not found in metadata
    """
    metadata = metadir._metadata

    if replace:
        log.warning(f"Replacing metadata for `{filebackend}` ...")
        metadata.delete()
    else:
        log.info(f"Updating metadata for `{filebackend}` ...")

    # use a consistent timestamp for state diff queries
    ts = datetime.now()

    # either read in json metadata files or actual files (only local filesystem here)
    if no_meta:
        files = _load_files(
            filebackend.get_children(condition=lambda x: "_mmmeta" not in x)
        )
    else:
        files = (
            _load_metadata(fp, metadir, ts, ensure_files)
            for _, fp in filebackend.get_children(
                condition=lambda x: x.endswith(".json")
            )
        )

    with dataset.connect("sqlite:///:memory:") as tx:
        metadb = _get_table(tx, metadir.config.unique)
        metadata.load(metadb)
        log.info(f"{metadb.count()} existing files.")

        # keep old state
        old_state = _get_table(tx, metadir.config.unique, "old_state")
        old_state.insert_many([i for i in metadb])

        res = _upsert(tx, metadir, files, "meta", ts, ensure_metadata)

        # export diff to append only db
        diff = _get_table(tx, metadir.config.unique, "diff")
        for file in metadb.find(__meta_last_updated=ts):
            old_file = ensure_dict(
                old_state.find_one(
                    **{metadir.config.unique: file[metadir.config.unique]}
                )
            )
            changed = dict(dict_diff(file, old_file))
            diff_data = {
                **changed,
                **{metadir.config.unique: file[metadir.config.unique]},
                **{"__mmmeta_keys": ",".join(changed.keys())},
            }
            diff.insert(diff_data)
        metadata.write(diff)

    if any(res[:4]):
        # added or updated:
        metadir.touch("meta_last_updated")

    return res
