Available backends
==================

Local watcher
-------------
.. automodule:: pytroll_watchers.local_watcher
   :members:

Minio bucket notification watcher
---------------------------------
.. automodule:: pytroll_watchers.minio_notification_watcher
   :members:

S3 bucket notification watcher
------------------------------
.. automodule:: pytroll_watchers.s3_poller
   :members:

Copernicus dataspace watcher
----------------------------
.. automodule:: pytroll_watchers.dataspace_watcher
   :members:

EUMETSAT datastore watcher
--------------------------
.. automodule:: pytroll_watchers.datastore_watcher
   :members:

DHuS watcher
------------
.. automodule:: pytroll_watchers.dhus_watcher
   :members:

Adding a new backend
--------------------
The base concept of the pytroll watchers library is very simply the publishing of file events. In order to add a new backend, two things need to be done:

1. Implement a generator that iterates over filesystem events and generates a pair of (file item, file metadata). The file item is the the (U)Path object to the file (check out universal_pathlib). The file metadata is a dictionary that contains the metadata of the file. It can be formatted in the same fashion as the message config. If it does not contain a `data` key, it is expected to be the contents of the data key itself (ie `subject` and `atype` will not be used as message parameters, but rather as `data` items).

2. Add an entry point to the new backend module. This module is expected to implement a `file_publisher` that just takes the config dictionary as argument.
