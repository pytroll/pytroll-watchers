.. pytroll-watchers documentation master file, created by
   sphinx-quickstart on Tue Apr  9 18:57:44 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pytroll-watchers's documentation!
============================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Pytroll-watcher is a library and command-line tool to detect changes on a local or remote file system.

At the moment we support local filesystems and Minio S3 buckets through bucket notifications.

CLI
***

The command-line tool can be used by invoking `pytroll-watcher <config-file>`. An example config-file can be::

   backend: minio
   fs_config:
     endpoint_url: my_endpoint.pytroll.org
     bucket_name: satellite-data-viirs
     storage_options:
       profile: profile_for_credentials
   publisher_config:
     name: viirs_watcher
   message_config:
     subject: /segment/viirs/l1b/
     atype: file
     data:
       sensor: viirs
     aliases:
       platform_name:
         npp: Suomi-NPP


API
***

Main interface
--------------
.. automodule:: pytroll_watchers
   :members:

Local watcher
-------------
.. automodule:: pytroll_watchers.local_watcher
   :members:

Minio bucket notification watcher
---------------------------------
.. automodule:: pytroll_watchers.minio_notification_watcher
   :members:

Testing utilities
-----------------
.. automodule:: pytroll_watchers.testing
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
