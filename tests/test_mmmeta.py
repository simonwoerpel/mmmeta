import copy
import os
import unittest
from datetime import datetime
from importlib import reload

from sqlalchemy.exc import IntegrityError
import yaml
from dataset.database import Database
from dataset.table import Table

from mmmeta import mmmeta, settings
from mmmeta.backend.filesystem import FilesystemBackend
from mmmeta.backend.store import Store
from mmmeta.config import Config
from mmmeta.exceptions import StoreError, ValidationError
from mmmeta.file import File
from mmmeta.metadir import Metadir


CONFIG = {
    "metadata": {
        "unique": "content_hash",
        "dedup": {"unique": "foreign_id", "max": "published_at"},
        "file_name": "_file_name",
        "include": [
            "reference",
            "modified_at",
            "title",
            "originators",
            "publisher:name",
        ],
    },
}


def create_config(data=""):
    with open("./testdata/_mmmeta/config.yml", "w") as f:
        yaml.dump(data, f)


class Test(unittest.TestCase):
    def setUp(self):
        # FIXME test db transactions?
        self.meta = mmmeta("./testdata/")
        self.meta.generate(replace=True)
        self.meta.update(replace=True)

    def test_init_via_env(self):
        m = mmmeta()
        self.assertEqual(m._base_path, os.getcwd())
        os.environ["MMMETA"] = "./testdata"
        reload(settings)
        m = mmmeta()
        self.assertIn("testdata", m._base_path)
        self.assertEqual(m._base_path, settings.MMMETA)

    def test_init(self):
        meta = self.meta
        backend = meta._backend
        store = meta.store
        self.assertIsInstance(meta, Metadir)
        self.assertIsInstance(backend, FilesystemBackend)
        self.assertIsInstance(store, Store)
        self.assertIsInstance(store._backend, FilesystemBackend)
        self.assertIn("testdata/_mmmeta", repr(meta))
        self.assertIn("testdata/_mmmeta", repr(backend))
        self.assertIn("testdata/_mmmeta/_store", repr(store))
        self.assertIn("testdata/_mmmeta/_store", str(store))
        self.assertIn("testdata/_mmmeta", backend.get_base_path())
        self.assertTrue(os.path.exists("./testdata/_mmmeta"))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store"))

    def test_store(self):
        store = self.meta.store
        store["foo"] = "bar"
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/foo"))
        self.assertEqual(store["foo"], "bar")
        with open("./testdata/_mmmeta/_store/hello", "w") as f:
            f.write("world")
        self.assertEqual(store["hello"], "world")
        self.assertIsInstance(store.serialize(), dict)
        # keys cannot contain "/"
        self.assertRaises(StoreError, lambda: store.__getitem__("illegal/key"))
        self.assertRaises(StoreError, lambda: store.__setitem__("illegal/key"))
        # values are typed
        store["counter"] = "1"
        store["value"] = 1.1
        self.assertIsInstance(store["counter"], int)
        self.assertIsInstance(store["value"], float)
        # touch shorthand
        self.meta.touch("my_timestamp")
        self.assertGreaterEqual(datetime.now(), self.meta.store["my_timestamp"])

    def test_dbs(self):
        meta = self.meta
        self.assertIsInstance(meta._state_db, Database)
        self.assertIsInstance(meta._meta_db, Database)
        self.assertIsInstance(meta.files._table, Table)
        self.assertEqual(meta._state_db.url, meta._db.url)
        self.assertTrue(os.path.exists("./testdata/_mmmeta/state.db"))
        # ensure primary key
        self.assertIn(meta.config.unique, meta.files.table.primary_key.columns.keys())
        self.assertIn("content_hash", meta.files.table.primary_key.columns.keys())

        # hacky change of primary key
        config = copy.deepcopy(CONFIG)
        config["metadata"]["unique"] = "_file_name"
        create_config(config)
        m = mmmeta("./testdata")
        m.generate()
        m.update()
        self.assertIn("_file_name", meta.files.table.primary_key.columns.keys())
        # reset config
        create_config(CONFIG)

    def test_generate(self):
        meta = self.meta
        # generate meta db
        meta.generate()
        # generate state db
        meta.update()

        # meta generation before state generation
        self.assertGreater(meta.state_last_updated, meta.meta_last_updated)

        testfiles = list(
            meta._backend.get_children("..", lambda x: x.endswith(".json"))
        )
        self.assertEqual(len(testfiles), len(meta._meta_db["files"]))
        self.assertEqual(len(testfiles), len(meta))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/meta_last_updated"))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/state_last_updated"))

    def test_file(self):
        meta = self.meta
        file = [f for f in meta.files][0]
        self.assertIsInstance(file, File)
        self.assertEqual(file.uid, file["content_hash"])
        # change file state
        file["imported"] = True
        file.save()
        imported_files = [f for f in meta.files.find(imported=True)]
        self.assertEqual(len(imported_files), 1)
        imported_file = imported_files[0]
        self.assertIsInstance(imported_file, File)
        self.assertDictEqual(imported_file._data, file._data)
        self.assertIn("__meta_last_updated", imported_file)
        self.assertIn("__meta_added", imported_file)
        self.assertIn("__state_added", imported_file)
        self.assertIn("__state_last_updated", imported_file)
        # file was added to state db after being added to meta db
        self.assertGreater(file["__state_added"], file["__meta_added"])
        # a file update always updates the metadir state
        self.assertGreaterEqual(meta.state_last_updated, file["__state_last_updated"])
        self.assertGreaterEqual(meta.last_touched, meta.state_last_updated)
        self.assertIn(file, meta.files)
        # add a new file
        meta.files.insert({"content_hash": "foo"})
        self.assertEqual(len(self.meta.files), 11)

    def test_config(self):
        m = self.meta
        self.assertIsInstance(m.config, Config)
        self.assertIsNone(m.config["foo"])
        # work with actual config:
        create_config(CONFIG)
        m = mmmeta("./testdata")
        for k, v in CONFIG.items():
            self.assertIn(k, m.config)
            self.assertEqual(m.config[k], v)
        for key in ("published_at", "foreign_id", "_file_name", "content_hash"):
            self.assertIn(key, m.config.required_keys)

    def test_validation(self):
        # metadata validation
        filedata = {"foo": "bar"}
        with self.assertRaises(ValidationError):
            self.meta.files.validate(filedata)
        for file in self.meta.files:
            self.assertTrue(self.meta.files.validate(file))

        # all valid if no config given
        create_config()  # empty config
        m = mmmeta("./testdata")
        m.generate()
        m.update()
        for file in m.files:
            self.assertTrue(m.files.validate(file))

        # more validation
        create_config(CONFIG)
        m = mmmeta("./testdata")
        m.generate(replace=True)
        m.update(replace=True)
        for file in m.files:
            self.assertTrue(m.files.validate(file))
            # only keys from meta config are present in database for each file
            file_keys = set(
                filter(
                    lambda x: not (x.startswith("__") or x == "id"), file._data.keys()
                )
            )
            self.assertSetEqual(file_keys, m.config.keys)

        # add an invalid file
        # missing primary key
        with self.assertRaises(IntegrityError):
            m._meta_db["files"].insert({"foo": "bar"})
        # primary key but missing required keys
        m._meta_db["files"].insert({"content_hash": "123", "foo": "bar"})
        with self.assertLogs(level="ERROR") as cm:
            invalid = m.update()[2]
        self.assertIn("Missing keys", cm.output[0])
        self.assertEqual(invalid, 1)

    def test_remote(self):
        config = {
            "metadata": {"file_name": "_file_name"},
            "remote": {
                "url": "https://my_bucket.s3.eu-central-1.amazonaws.com/foo/bar/{_file_name}",  # noqa
                "uri": "s3://my_bucket/foo/bar/{_file_name}",
            },
        }
        create_config(config)
        m = mmmeta("./testdata")
        m.generate(replace=True)
        m.update()
        for file in m.files:
            self.assertIn("amazonaws", file.remote.url)
            self.assertIn(file.name, file.remote.url)
            self.assertTrue(file.remote.url.endswith(file.name))
            self.assertTrue(file.remote.uri.startswith("s3://my_bucket"))

    def test_delete_file(self):
        # move file away
        file = "./testdata/0011d580dcdff07f0c3a95ddc80b8fd545faa7d6.json"
        os.rename(file, "/tmp/test.json")
        with self.assertLogs(level="WARNING") as cm:
            res = self.meta.generate(ensure=True)
        self.assertEqual(res[3], 1)
        self.assertIn("soft deleted files in meta.db", cm.output[0])

        # this doesn't return delete count as the deleted file
        # still exists (marked as deleted) in the meta db:
        # but we are logging it
        with self.assertLogs(level="WARNING") as cm:
            res = self.meta.update()
        self.assertEqual(res[3], 0)
        self.assertIn("soft deleted files in meta.db", cm.output[0])

        # actual delete file from meta db
        self.meta._meta_db["files"].delete(
            content_hash="0011d580dcdff07f0c3a95ddc80b8fd545faa7d6"
        )
        self.assertEqual(self.meta.generate(replace=True)[1], 9)
        with self.assertLogs(level="WARNING") as cm:
            res = self.meta.update()
        self.assertEqual(res[3], 1)
        self.assertIn("hard deleted files in meta.db", cm.output[1])

        # move back for further tests
        os.rename("/tmp/test.json", file)

    def test_generate_no_meta(self):
        # generate metadir from actual files, no json metadata
        create_config(
            {
                "metadata": {
                    "unique": "content_hash",
                    "file_name": "file_name",
                },
            }
        )
        m = mmmeta("./testdata")
        m.generate(replace=True, no_meta=True)
        m.update()
        # now we just read in all the files (json and pdf) = 20
        self.assertEqual(m.files.count(), 20)
        for file in m.files:
            for key in (
                "file_name",
                "file_path",
                "file_size",
                "created_at",
                "modified_at",
                "content_hash",
            ):
                self.assertIn(
                    key,
                    file._data.keys(),
                )

        # reset config
        create_config()
