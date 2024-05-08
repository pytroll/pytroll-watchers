Published messages
******************

The published messages will contain information on how to access the resource advertized. The following parameters will
be present in the message.

Resource location information
=============================

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

.. warning::

   Pytroll-watchers tries to prevent publishing of sensitive information such as passwords and secret keys, and will
   raise an error in most cases when this is done. However, always double-check your pytroll-watchers configuration so
   that secrets are not passed to the library to start with.
   Solutions include ssh-agent for ssh-based filesystems, storing credentials in .aws config files for s3 filesystems.
   For http-based filesystems implemented in pytroll-watchers, the username and password are used to generate a token
   prior to publishing, and will thus not be published.

path
----

This parameter is the companion to `filesystem` and gives the path to the resource within the filesystem.

Examples of paths:

 - `/viirs-data/sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `/sdr/SVM13_npp_d20240408_t1006227_e1007469_b64498_c20240408102334392250_cspp_dev.h5`
 - `/files/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiances.nc`

Other metadata
==============

Other metadata items are provided when possible:

* boundary: the geojson boundary of the data
* platform_name
* sensor
* orbit_number
* start_time
* end_time
* product_type
* checksum
