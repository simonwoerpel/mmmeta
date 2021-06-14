import csv
from datetime import datetime

import dataset

from ..util import robust_dict
from .filesystem import FilesystemBackend, ensure_directory


class AppendOnlyBackend(FilesystemBackend):
    def __init__(self, base_path, unique):
        super().__init__(base_path)
        self.unique = unique

    def _get_table(self, tx, name="tmp"):
        return tx.get_table(name, primary_id=self.unique, primary_type=tx.types.text)

    def write(self, table, suffix="append"):
        fp = self.get_path(datetime.now().isoformat() + f".{suffix}")
        ensure_directory(self.base_path)  # FIXME
        if hasattr(table, "all"):  # FIXME
            table = table.all()
        try:
            # maybe we have 0 rows:
            data = next(table)
            with open(fp, "w") as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
                for data in table:
                    writer.writerow(data)
        except StopIteration:
            pass

    def load_step(self, path):
        path = self.get_path(path)
        with open(path) as f:
            reader = csv.DictReader(f)
            yield from reader

    def load(self, table):
        """
        files are named by timestamp, so we can order the history
        walk up until the most recent squashed, then walk back down
        from there to generate data
        """

        def _get_steps():
            steps = reversed(sorted(c[1] for c in self.get_children()))
            for step in steps:
                if step.endswith("append"):
                    yield step
                if step.endswith("squashed"):
                    yield step
                    break

        for step in reversed(list(_get_steps())):
            for row in self.load_step(step):
                row = robust_dict(row)
                keys = row.get("__mmmeta_keys")
                if keys:
                    keys = keys.split(",") + [self.unique]
                    row = {k: v for k, v in row.items() if k in keys}
                table.upsert(row, [self.unique])

    def squash(self):
        with dataset.connect("sqlite:///:memory:") as tx:
            table = self._get_table(tx)
            self.load(table)
            self.write(table, "squashed")
