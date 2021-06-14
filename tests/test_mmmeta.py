import copy
import csv
import json
import os
import shutil
import unittest
from datetime import datetime
from importlib import reload

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
        "file_name": "_file_name",
        "required": ["foreign_id", "published_at"],
        "include": [
            "reference",
            "modified_at",
            "published_at",
            "title",
            "originators",
            "publisher:name",
            "int_value",
            "bool_value",
        ],
    },
}


def create_config(data=""):
    os.makedirs("./testdata/_mmmeta", exist_ok=True)
    with open("./testdata/_mmmeta/config.yml", "w") as f:
        yaml.dump(data, f)


class Test(unittest.TestCase):
    def get_m(self, config=""):
        """
        init a fresh instance
        """
        create_config(config)
        m = mmmeta("./testdata")
        m.generate(replace=True)
        m.update(replace=True)
        return m

    def setUp(self):
        if os.path.exists("./testdata.BCKP"):
            shutil.rmtree("./testdata.BCKP/")
        shutil.copytree("./testdata", "./testdata.BCKP")

    def tearDown(self):
        shutil.rmtree("./testdata/")
        shutil.move("./testdata.BCKP", "./testdata")

    def test_init_via_env(self):
        m = mmmeta()
        self.assertEqual(m._base_path, os.getcwd())
        os.environ["MMMETA"] = "./testdata"
        reload(settings)
        m = mmmeta()
        self.assertIn("testdata", m._base_path)
        self.assertEqual(m._base_path, settings.MMMETA)
        self.assertIn("testdata", m._files_root)
        self.assertEqual(m._files_root, settings.MMMETA_FILES_ROOT)
        os.environ["MMMETA_FILES_ROOT"] = "./testdata/foo"
        reload(settings)
        m = mmmeta()
        self.assertIn("testdata", m._base_path)
        self.assertEqual(m._base_path, settings.MMMETA)
        self.assertIn("foo", m._files_root)
        self.assertEqual(m._files_root, settings.MMMETA_FILES_ROOT)

    def test_init(self):
        meta = self.get_m()
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
        self.assertTrue(os.path.exists("./testdata/_mmmeta/db"))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store"))
        # other initialization method: mmmeta_root, files_root
        m = mmmeta("foo", "bar")
        self.assertIn("foo", m._base_path)
        self.assertIn("bar", m._files_root)

    def test_store(self):
        m = self.get_m()
        store = m.store
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
        m.touch("my_timestamp")
        self.assertGreaterEqual(datetime.now(), m.store["my_timestamp"])

    def test_dbs(self):
        meta = self.get_m(CONFIG)
        self.assertIsInstance(meta._db, Database)
        self.assertIsInstance(meta.files._table, Table)
        self.assertTrue(os.path.exists("./testdata/_mmmeta/state.db"))
        # ensure primary key
        self.assertIn(meta.config.unique, meta.files.table.primary_key.columns.keys())
        self.assertIn("content_hash", meta.files.table.primary_key.columns.keys())

        # hacky change of primary key
        config = copy.deepcopy(CONFIG)
        config["metadata"]["unique"] = "_file_name"
        create_config(config)
        m = mmmeta("./testdata")
        m.generate(replace=True)
        m.update()
        self.assertIn("_file_name", meta.files.table.primary_key.columns.keys())

    def test_generate(self):
        meta = mmmeta("./testdata")
        # generate meta db
        meta.generate()
        # generate state db
        meta.update()

        # meta generation before state generation
        self.assertGreater(meta.state_last_updated, meta.meta_last_updated)

        testfiles = list(
            meta._backend.get_children("..", lambda x: x.endswith(".json"))
        )
        self.assertEqual(len(testfiles), len(meta._db["files"]))
        self.assertEqual(len(testfiles), len(meta))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/meta_last_updated"))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/state_last_updated"))

        # no change
        with self.assertLogs(level="INFO") as cm:
            meta.generate()
        count = len(meta.files)
        self.assertIn(f"INFO:mmmeta.db:Skipped {count} not changed files.", cm[1])

    def test_file(self):
        meta = self.get_m()
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
        self.assertEqual(len(meta.files), 11)

    def test_config(self):
        m = self.get_m()
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
        m = self.get_m()
        filedata = {"foo": "bar"}
        with self.assertRaises(ValidationError):
            m.files.validate(filedata)

        # all valid if no config given
        m = self.get_m()  # empty config
        for file in m.files:
            self.assertTrue(m.files.validate(file))

        # more validation
        m = self.get_m(CONFIG)
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
        with open("./testdata/invalid.json", "w") as f:
            json.dump({"foo": 1}, f)
        with self.assertLogs(level="ERROR") as cm:
            invalid = m.generate()[2]
        self.assertIn("Missing keys", cm.output[0])
        self.assertEqual(invalid, 1)
        # primary key but missing required keys
        with open("./testdata/invalid.json", "w") as f:
            json.dump({"content_hash": "123", "foo": "bar"}, f)
        with self.assertLogs(level="ERROR") as cm:
            invalid = m.generate()[2]
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
        m = self.get_m(config)
        for file in m.files:
            self.assertIn("amazonaws", file.remote.url)
            self.assertIn(file.name, file.remote.url)
            self.assertTrue(file.remote.url.endswith(file.name))
            self.assertTrue(file.remote.uri.startswith("s3://my_bucket"))

    def test_delete_file(self):
        # remove metadata file
        m = self.get_m()
        os.remove("./testdata/0011d580dcdff07f0c3a95ddc80b8fd545faa7d6.json")
        with self.assertLogs(level="WARNING") as cm:
            res = m.generate(ensure_metadata=True)
        self.assertEqual(res[3], 1)
        self.assertIn("soft deleted files", cm.output[0])

        # soft delete
        with self.assertLogs(level="WARNING") as cm:
            res = m.update()
        self.assertEqual(res[3], 1)
        self.assertIn("soft deleted files", cm.output[0])

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
        m.update(replace=True)
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

    def test_other_files_root(self):
        # assert that no files are found in different root location
        m = mmmeta("./testdata", "./testdata/other_files")
        m.generate(replace=True)
        m.update(replace=True)
        self.assertEqual(m._db["files"].count(), 0)

    def test_ensure_actual_files(self):
        m = self.get_m(CONFIG)
        # remove actual file
        os.remove("./testdata/0011d580dcdff07f0c3a95ddc80b8fd545faa7d6.data.pdf")
        with self.assertLogs(level="WARNING") as cm:
            m.generate(ensure_files=True)
        # the file is now soft deleted
        self.assertIn("1 soft deleted files", cm.output[0])
        with self.assertLogs(level="WARNING") as cm:
            res = m.update()
        self.assertEqual(res[3], 1)
        self.assertIn("1 soft deleted files", cm.output[0])

    def test_robust_dict(self):
        # use `util.robust_dict` for comparison performance
        m = self.get_m(CONFIG)
        res = m.generate()
        # nothing changed:
        self.assertEqual(res[-1], 10)
        res = m.update()
        # still nothing changed:
        self.assertEqual(res[-1], 10)

    def test_typing(self):
        m = self.get_m(CONFIG)
        file = m.files.find_one(content_hash="0011d580dcdff07f0c3a95ddc80b8fd545faa7d6")
        self.assertIsInstance(file["int_value"], int)
        self.assertIsInstance(file["bool_value"], str)  # FIXME
        self.assertEqual(file["bool_value"], "True")
        self.assertIsInstance(file["__meta_last_updated"], datetime)
        self.assertIsInstance(file["__state_last_updated"], datetime)

    def test_diff_update(self):
        m = self.get_m(CONFIG)
        file = m.files.find_one(content_hash="0011d580dcdff07f0c3a95ddc80b8fd545faa7d6")
        self.assertEqual(file["int_value"], 2)
        # only one metadata db csv file
        csv_files = list(m._metadata.get_children())
        self.assertEqual(len(csv_files), 1)
        # explicitly change metadata of 1 file
        data = m._backend.load_json("../0011d580dcdff07f0c3a95ddc80b8fd545faa7d6.json")
        data["int_value"] = 3
        m._backend.dump_json("../0011d580dcdff07f0c3a95ddc80b8fd545faa7d6.json", data)
        m.generate()
        # now 2 csv files
        csv_files = list(m._metadata.get_children())
        self.assertEqual(len(csv_files), 2)
        last_csv = sorted(csv_files)[-1]
        with open(last_csv[1]) as f:
            reader = csv.DictReader(f)
            data = [r for r in reader]
        # only 1 file updated
        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertIn("int_value", data.keys())
        # only the updated keys are in the csv
        self.assertSetEqual(
            set(("content_hash", "__meta_last_updated", "int_value")), set(data.keys())
        )
        # update again, nothing changes
        m.generate()
        csv_files = list(m._metadata.get_children())
        self.assertEqual(len(csv_files), 2)
        m.update()
        file = m.files.find_one(content_hash="0011d580dcdff07f0c3a95ddc80b8fd545faa7d6")
        self.assertEqual(file["int_value"], 3)
