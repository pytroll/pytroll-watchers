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
