from uuid import uuid4
import os
import shutil
import unittest
from datetime import datetime

import dataset

from mmmeta.backend.appendonly import AppendOnlyBackend


class Test(unittest.TestCase):
    def setUp(self):
        if os.path.exists("./testdata/aof"):
            shutil.rmtree("./testdata/aof")

    def tearDown(self):
        if os.path.exists("./testdata/aof"):
            shutil.rmtree("./testdata/aof")

    def test_appendonly(self):
        backend = AppendOnlyBackend("./testdata/aof", unique="uid")

        with dataset.connect("sqlite:///:memory:") as tx:
            table = tx["data"]
            for i in range(1000):
                table.insert(
                    {"uid": i, "foo": "bar", "ts": datetime.now(), "data": str(uuid4())}
                )
            backend.write(table)

        children = list(backend.get_children())
        self.assertEqual(len(children), 1)

        with dataset.connect("sqlite:///:memory:") as tx:
            table = tx["data"]
            for i in range(10000):
                table.insert(
                    {
                        "uid": i,
                        "foo": "bar2",
                        "ts": datetime.now(),
                        "more_data": str(uuid4()),
                    }
                )
            backend.write(table)

        children = list(backend.get_children())
        self.assertEqual(len(children), 2)

        with dataset.connect("sqlite:///:memory:") as tx:
            table = tx["data"]
            backend.load(table)
            self.assertEqual(len(table), 10000)

        backend.squash()

        children = list(sorted(backend.get_children()))
        self.assertEqual(len(children), 3)
        self.assertIn(".squashed", children[-1][1])

        with dataset.connect("sqlite:///:memory:") as tx:
            table = tx["data"]
            backend.load(table)
            self.assertEqual(len(table), 10000)
            self.assertSetEqual(
                set(table.columns), set(("id", "uid", "data", "more_data", "foo", "ts"))
            )
            # foo=bar was overriden in 2nd append
            empty = [i for i in table.find(foo="bar")]
            self.assertEqual(len(empty), 0)
