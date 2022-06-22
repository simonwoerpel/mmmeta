|test| |release| |pypi|

mmmeta
======

``mmmeta`` is a command-line toolkit and python library to incrementally
synchronize file metadata between an **archive** that stores all files
and their metadata, **publishers** that add new files to the archive and
**consumers** that process these files (or a subset of them).

It’s better explained by a concrete example:

**Publisher** incrementally scrapes documents and stores them with
metadata in the **archive**.

**Consumer** wants to import some files by a given filter criterion and
keep track of the ones that are already imported.

As such file collections grow, we only want to transfer as less data as
possible between **archive**, **publisher** and **consumer**.

synopsis
--------

To clarify the terms used in this manual:

-  **files**: actual files (like pdfs…)
-  **metadata files**: json files that contain metadata for actual files
-  **archive**: the “source of truth” where files, metadata files and
   metadata db are stored.
-  **publisher**: an application that adds new files to the **archive**
-  **consumer**: an application that processes the files from
   **archive** (with read-only access)
-  **state db**: sqlite database, stored only on **consumers**, tracking
   local state for files
-  **metadir**: a directory named ``_mmmeta`` that is
   `synced <#synchronization>`__ between **archive**, **publishers** and
   **consumers**
-  `store <#store>`__: A simple implementation of a key-value store for
   additional information

Usage
-----

Archive
~~~~~~~

The archive can be any file-like (remote) location for the actual files,
their *metadata files* and the *metadir*. **Publishers** would need
write access to it, **Consumers** only need read-only.

``mmmeta`` usually doesn’t operate on the archive itself (as it would be
most likely just a data bucket), instead, maintaining the archive is
done by **publishers**

Publisher
~~~~~~~~~

An application that writes to the archive. This can be for example a
scraper that incrementally adds new files.

The usual workflow would look like this:

1. `synchronize <#synchronization>`__ *metadir* from archive
2. Run application (e.g. scraper) optionally based on synced metadata
3. Update *metadir* (see below)
4. `synchronize <#synchronization>`__ *metadir* back to archive

updating *metadir*
^^^^^^^^^^^^^^^^^^

::

   mmmeta generate

This will loop through all json files in the current directory and
create or add csv data in ``./_mmmeta/db/``

For other path locations, see `initialization <#initialization>`__

managing files presence
^^^^^^^^^^^^^^^^^^^^^^^

Per default, ``mmmeta generate`` only adds new files based on the
*metadata files* available (it doesn’t even check the presence of the
actual files). To “clean up” (e.g. delete non-existing files), the cli
interface provides the following options:

::

     --replace       Completly replace the meta database
     --ensure        Ensure metadata files are present, soft-delete non-existing
     --ensure-files  Ensure actual files are present (for local store only),
                     soft-delete non-existing
     --no-meta       Read in actual files instead of json metadata files

Consumer
~~~~~~~~

An application that processes the files, e.g. import them into a
database.

``mmmeta`` is used to merge *remote* metadata into the local *state db*
(sqlite) and provides some functionallity to query and manage this data
in applications.

The usual workflow would look like this:

1. `synchronize <#synchronization>`__ *metadir* from archive
2. Update local *state db* (see below)
3. Run application that alters local state (see example below)

update local state db
^^^^^^^^^^^^^^^^^^^^^

via cli:

::

   mmmeta update

or via python:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta()
   m.update()

For other path locations, see `initialization <#initialization>`__

consumer application
^^^^^^^^^^^^^^^^^^^^

The ``files`` object on a metadir is a wrapper to a `dataset
table <https://dataset.readthedocs.io/en/latest/api.html#table>`__ with
all its functionallity, with the addition that it yields
``mmmeta.file.File`` objects that have a bit extra functionality like
directly saving and access to “proxy values” (see config below)

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta()

   for file in m.files(document_type="contract", imported=False):
       download_url = file.remote.url  # see config below
       process_download(download_url)
       file["downloaded"] = True
       file.save()

See `config <#remote>`__ on how to generate remote urls or uris

Initialization
~~~~~~~~~~~~~~

When **mmmeta** is `initialized <#initialization>`__ with a path
argument named ``foo``, the directory ``foo/_mmmeta`` will be the
*metadir*

The path ``foo`` can be set via env var:

::

   MMMETA=./foo/ mmmeta update

or in scripts:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./foo/")

On `publishers <#publishers>`__ there is an additional env var
``MMMETA_FILES_ROOT`` if the location for the *actual files* is
different.

Synchronization
---------------

This package is totally agnostic about the remote storage backend (could
be a local filesystem location or cloud storage) and doesn’t handle any
of the local <-> remote synchronization.

Therefore the synchronization of the *metadir* ``./foo/_mmmeta`` is up
to you with the tool of your choice.

Config
------

``mmmeta`` can optionally have a config stored in
``./foo/_mmmeta/config.yml``

Example (all settings are optional):

.. code:: yaml

   metadata:
     file_name: _file_name  # key in json metadat for file name
     include:  # only include these keys from json metadata in meta db
     - reference
     - modified_at
     - title
     - originators
     - publisher:name  # nested keys are flattened with ":" between them
     unique: content_hash  # unqiue identifier for files
   remote:  # simple string replacement to generate `File.remote.<attr>` attributes, like:
     url: https://my_bucket.s3.eu-central-1.amazonaws.com/foo/bar/{_file_name}
     uri: s3://my_bucket/foo/bar/{_file_name}

remote
~~~~~~

The configuration section ``remote`` from above ensures that the file
objects have attributes to access the actual files from the remote:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta()

   for file in m.files:
       print(file.remote.uri)

Store
-----

``mmmeta`` ships with a simple key-value-store that can be used by both
the *remote* and *client* to store some additional data. The store lives
in the *metadir* ``./foo/_mmmeta/_store``

You can store any values in it:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir/")
   m.store["new_files"] = 17

any machine that `synchronizes <#synchronization>`__ the metadir can
read these values:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir/")
   new_files = m.store["new_files"]  # 17

For storing timestamps, there is a shorthand via the ``touch`` function:

.. code:: python

   m.touch("my_ts_key")

This will save the value of the current ``datetime.now()`` to the key
``my_ts_key``. The values are typed (``int``, ``float`` or
``timestamp``), so you can easily do something like this:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir/")

   if m.store["remote_last_updated"] > m.store["local_last_updated"]:
       # run scraper

Installation
------------

Requires python3. Virtualenv use recommended.

Additional dependencies will be installed automatically:

::

   pip install mmmeta

After this, you should be able to execute in your terminal:

::

   mmmeta --help

You should as well be able to import it in your python scripts:

.. code:: python

   from mmmeta import mmmeta

cli
---

.. code:: bash

   Usage: mmmeta [OPTIONS] COMMAND [ARGS]...

   Options:
     --metadir TEXT     Base path for reading meta info and storing state
                        [default: <current/working/dir>]
     --files-root TEXT  Base path for actual files to generate metadir from
                        [default: <current/working/dir>]
     --help             Show this message and exit.

   Commands:
     generate
     inspect
     update

developement
------------

Install testing requirements:

::

   make install

Test:

::

   make test

.. |test| image:: https://github.com/simonwoerpel/mmmeta/actions/workflows/test.yml/badge.svg
   :target: https://github.com/simonwoerpel/mmmeta/actions/workflows/test.yml
.. |release| image:: https://github.com/simonwoerpel/mmmeta/actions/workflows/release.yml/badge.svg
   :target: https://github.com/simonwoerpel/mmmeta/actions/workflows/release.yml
.. |pypi| image:: https://github.com/simonwoerpel/mmmeta/actions/workflows/publish.yml/badge.svg
   :target: https://github.com/simonwoerpel/mmmeta/actions/workflows/publish.yml
