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

It is also possible to fetch the file locally before continuing, with some information it the data config section::

  data_config:
    fetch:
      destination: /data/received_files


Unpacking of the file into it's component and subsequent publishing is achieved by passing the archive format
in the data config part, for example::

  data_config:
    unpack:
      format: zip

or for the case of a directory::

  data_config:
    unpack:
      format: directory
      include_dir_in_uid: true
