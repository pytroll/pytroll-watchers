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

Configuration
_____________

The configuration of pytroll watchers’ CLI is done through a yaml configuration
file representing a dictionary (key-value pairs) of options. Similarly different functions in the library use that same dictionary (a python object in that case) as input parameter.
This section should apply to both dictionaries, unless otherwise specified.

The configuration is expected to contain the following four items:

- `fs_config` for the configuration of the filesystem to watch. The parameters to provide here differ depending on what backend is being used, so the curious reader is recommended to look at the description for the corresponding backend.
- `publiser_config` for the configuration of the publishing part. These parameters are passed directly to posttroll’s `create_publisher_from_dict`, so here again, please refer to the corresponding documentation.
- `message_config` for the configuration of the message to send when a new filesystem object is to be published. Most parameters here are passed to posttroll’s `Message` constructor (check the documentation too), with one exception: `aliases` is removed from the items passed to the constructor and is used instead for replacing information from the file/object metadata. Say for example that the parsed file object from a local file system contains the `platform_name` field, and is set to a satellite short name `npp`. Providing an alias of the form `{platform_name: {npp: "Suomi-NPP"}}` will effectively set the platform name to "Suomi-NPP". Each value of the aliases dictionary is itself a dictionary, so multiple key-value pairs can be provided here to support multiple cases.
- `data_config` (optional) for the configuration of the "processing" to be done to the incoming filesystem events. At the moment, there are two possibilities: `fetch` for fetching remote data locally before advertising the event, and `unpack` to send the advertising the components of the event (e.g. in the case the user wants to advertise the contents of a receive zip file event). More information on how to format these options is shown in the examples above.

Finally, the last element for the case of the configuration file for the CLI is the top-level `backend` parameter which tells pytroll-watcher which backend to use. The list of available backends is available as entry points defined in the pyproject.toml file.
