import os
import unittest
from datetime import datetime
from importlib import reload

from dataset.database import Database
from dataset.table import Table

from mmmeta import mmmeta, settings
from mmmeta.backend.filesystem import FilesystemBackend
from mmmeta.backend.store import Store
from mmmeta.exceptions import StoreError
from mmmeta.file import File
from mmmeta.metadir import Metadir


class Test(unittest.TestCase):
    def generate_dbs(self):
        # FIXME test db transactions?
        self.meta.generate()
        self.meta.update()

    def setUp(self):
        self.meta = mmmeta("./testdata/")

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

        # implicit db creation (but empty)
        files = [f for f in meta.files]
        self.assertListEqual(files, [])
        self.assertTrue(os.path.exists("./testdata/_mmmeta/state.db"))

    def test_generate(self):
        meta = self.meta
        # generate meta db
        meta.generate()
        # generate state db
        meta.update()

        # meta generation before state generation
        self.assertGreater(meta.state_last_touched, meta.meta_last_touched)

        testfiles = list(
            meta._backend.get_children("..", lambda x: x.endswith(".json"))
        )
        self.assertEqual(len(testfiles), len(meta._meta_db["files"]))
        self.assertEqual(len(testfiles), len(meta))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/meta_last_updated"))
        self.assertTrue(os.path.exists("./testdata/_mmmeta/_store/state_last_updated"))

    def test_file(self):
        self.generate_dbs()  # FIXME
        meta = self.meta
        file = [f for f in meta.files][0]
        self.assertIsInstance(file, File)
        self.assertEqual(file.id, file["content_hash"])
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
        self.assertGreaterEqual(meta.state_last_touched, file["__state_last_updated"])
        self.assertGreaterEqual(meta.last_touched, meta.state_last_touched)
        self.assertIn(file, meta.files)
        # add a new file
        meta.files.insert({"content_hash": "foo"})
        self.assertEqual(len(self.meta.files), 11)
        # cleanup for other tests FIXME
        meta.files.delete(content_hash="foo")
