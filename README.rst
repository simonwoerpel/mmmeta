|test| |release| |pypi|

mmmeta
======

Handle meta information and local state about files from remote
(read-only) locations.

example usecase
---------------

It’s better explained by a concrete example:

**Server** scrapes documents and stores them with metadata

**Client1** wants to download all files with
``document_type="contract"``

**Client2** wants to import all documents scraped not longer than 1 week
ago into a database, but only the ones that are not imported yet

synopsis
~~~~~~~~

To clarify the terms used in this manual:

-  **files**: actual files (like pdfs…)
-  **metadata files**: json files that contain metadata for actual files
-  **metadata db**: sqlite database containing metadata for all files
   from the remote
-  **remote**: the “source of truth” where files, metadata files and
   metadata db are stored. A remote can still be a local folder on the
   same machine…
-  **client**: A client that has read-only access to the remote
-  **state db**: sqlite database, stored only on the client, containing
   local state for files
-  `store <#store>`__: A simple implementation of a key-value store for
   additional information
-  **metadir**: a directory named ``_mmmeta`` that is
   `synced <#synchronization>`__ between remote and client and contains
   metadata db, store, and (on the client) state db

how does this scenario work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Server** 1. Stores a metadata json file for each file 2. Generates
(and updates) a *metadir*

**Client1** 1. Syncs remote *metadir* 2. Merge remote *metadata db* with
local *state db* 3. Query *state db* for given criteria 4. For each
result download the actual file from the remote

**Client2** 1. Syncs remote *metadir* 2. Merge remote *metadata db* with
local *state db* 3. Query the *state db* for remote metadata
``retrieved_at=<date>`` and local state ``imported=False``

**mmmeta** automates *almost ;)* all of this:

implementation of this scenario with mmmeta
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Server
^^^^^^

scrapes document and stores them with metadata

a) Server stores files locally
''''''''''''''''''''''''''''''

If the files (and their metadata) are stored locally, metadata
generation is as easy as looping through all of the json files and
generate the database out of it. This can be done via command line
inside the directory of the *metadata files*:

::

   mmmeta generate

This will loop through all json files create a sqlite database in
``./_mmmeta/meta.db``

For other path locations, see `initialization <#initialization>`__

When new *metadata files* are added, simply re-run this command. It will
just update the *meta db* without deleting existing entries, which means
the old *metadata files* don’t need to stay on the server (see next
situation).

b) Server downloads files locally but then pushes into a cloud
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Here we don’t have all the files locally, only a subset (the new
downloaded ones).

First, `synchronize <#synchronization>`__ cloud *metadir* to local (aka
the server).

Then update metadata as described above:

::

   mmmeta generate

Last, `synchronize <#synchronization>`__ the updated *metadir* back to
the cloud.

c) Server directly pushes files to cloud
''''''''''''''''''''''''''''''''''''''''

Here, we don’t have any file and its metadata locally (on the server).
Updating the *meta db* happens within the python code of the
application:

First, `synchronize <#synchronization>`__ cloud *metadir* to local (aka
the server).

Then, run your application…

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir")

   for data in scraper:
       m.files.insert(**data)
       # or upsert, if you want:
       m.files.upsert(**data, [keys])  # e.g. "content_hash"

This will update the *meta db* in the *metadir*

Last, `synchronize <#synchronization>`__ the updated *metadir* back to
the cloud.

Client1
^^^^^^^

wants to download all files with ``document_type="contract"``

First, `synchronize <#synchronization>`__ remote *metadir* to local.

Then,

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir")

   for file in m.files(document_type="contract"):
       download(file.public.url)

   def download(url):
       # implement download based on remote storage
       # url will be, based on storage, something like:
       # - file:///path/to/file.pdf (remote is local filesystem)
       # - s3://bucket/path/to/file.pdf (remote is aws cloud storage)
       # - https://remote.com/path/to/file.pdf
       # ...

See `config <#public>`__ on how to generate public urls or uris

The

Client2
^^^^^^^

wants to import all documents scraped not longer than 1 week ago into a
database, but only the ones that are not imported yet

Therefore the client uses a local state db in the mmmeta.

First, `synchronize <#synchronization>`__ remote metadata db to local

Then, update meta to local state: via command-line:

::

   MMMETA=./path/to/metadir mmmeta update

or programmatically:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/to/metadir/")
   m.update()

After that, remote metadata and local state are merged and easy usable
like this:

.. code:: python

   for file in m.files.find(retrieved_at=<date>, imported=False):
       process_import(file)
       file["imported"] = True
       file.save()

The ``files`` object on a metadir is a wrapper to a `dataset
table <https://dataset.readthedocs.io/en/latest/api.html#table>`__ with
all its functionallity, instead that it yields ``File`` objects that you
can use to alter the state of the files in the database as described in
the example above.

Initialization
~~~~~~~~~~~~~~

On the *client*:

When **mmmeta** is `initialized <#initialization>`__ with a ``path``,
the directory ``path/_mmmeta`` will be the *metadir*

``path`` can be set via env var:

::

   MMMETA=./path/ mmmeta update

or in scripts:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta("./path/")

On the *remote*:

Same as client, but for the *metadata files* either recursively inside
``path`` unless other specified via env var ``MMMETA_FILES_ROOT``

This means, on the *remote* the *metadata files* and the *metadir* don’t
need to be in the same path location.

Or, speaking of clouds: *metadir* and *actual files* can exist in
different buckets.

Synchronization
^^^^^^^^^^^^^^^

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
   public:  # simple string replacement to generate `File.public.<attr>` attributes, like:
     url: https://my_bucket.s3.eu-central-1.amazonaws.com/foo/bar/{_file_name}
     uri: s3://my_bucket/foo/bar/{_file_name}

public
~~~~~~

The configuration section ``public`` from above ensures that the file
objects have attributes to access the actual files from the remote:

.. code:: python

   from mmmeta import mmmeta

   m = mmmeta()

   for file in m.files:
       print(file.public.uri)

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
