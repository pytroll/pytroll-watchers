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

Published messages
******************

The published messages will contain information on how to access the resource advertized. The following parameters will
be present in the message.

uid
---

This is the unique identifier for the resource. In general, it is the basename for the file/objects, since we assume
that two files with the same name will have the same content. In some cases it can include the containing directory.

Examples of uids:

 - `SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiances.nc`

uri
---

This is the URI that can be used to access the resource. The URI can be composed as fsspec allows for more complex cases.

Examples of uris:

 - `s3://viirs-data/sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `zip://sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5::s3://viirs-data/viirs_sdr_npp_d20240408_t1006227_e1007469_b64498.zip`
 - `https://someplace.com/files/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiances.nc`

 filesystem
 ----------

 Sometimes the URI is not enough to gain access to the resource, for example when the hosting service requires
 authentification. This is why pytroll-watchers with also provide the filesystem and the path items. The filesystem
 parameter is the fsspec json representation of the filesystem. This can be used on the recipient side using eg::

   fsspec.AbstractFileSystem.from_json(json.dumps(fs_info))

where `fs_info` is the content of the filesystem parameter.

To pass authentification parameters to the filesystem, use the `storage_options` configuration item.


Example of filesystem:

 - `{"cls": "s3fs.core.S3FileSystem", "protocol": "s3", "args": [], "profile": "someprofile"}`

Note:

   Pytroll-watchers tries to prevent publishing of sensitive information such as passwords and secret keys, and will
   raise an error in most cases when this is done. However, always double-check your pytroll-watchers configuration so
   that secrets are not passed to the library to start with.

path
----

This parameter is the companion to `filesystem` and gives the path to the resource within the filesystem.

Examples of paths:

 - `/viirs-data/sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `/sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `/files/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiances.nc`


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

Copernicus dataspace watcher
---------------------------------
.. automodule:: pytroll_watchers.dataspace_watcher
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
