"""Watcher for non-remote file systems.

Either using OS-based envents (like inotify on linux), or polling.

An example configuration file to retrieve data from a directory.

By default, files uris will not include any protocol, which means they will look like `/tmp/myfile`. If it is desired,
the `protocol` setting in the `fs_config` can be set to `"file"` to make the uris look like `file:///tmp/myfile`.

It is also possible to make the local files sent as remote with the `protocol` and `storage_options` settings in the
`fs_config` section. The generated uri can thus start with `ssh://myhost` for example, by setting `protocol: ssh` and
`storage_options: {host: "myhost"}`.


.. code-block:: yaml

  backend: local
  fs_config:
    directory: /data
    file pattern: "H-000-{orig_platform_name:4s}__-{orig_platform_name:4s}_{service:3s}____-{channel_name:_<9s}-\
        {segment:_<9s}-{start_time:%Y%m%d%H%M}-{compression:1s}_"
  publisher_config:
    name: hrit_watcher
  message_config:
    subject: /segment/hrit/l1b/
    atype: file

"""
import logging
import os
from pathlib import Path
from urllib.parse import urlunparse

from upath import UPath

from pytroll_watchers.backends.local import listen_to_local_events
from pytroll_watchers.publisher import SecurityError, file_publisher_from_generator, parse_metadata

logger = logging.getLogger(__name__)


def file_publisher(config):
    """Publish files coming from local filesystem events.

    Args:
        config: the configuration dictionary, containing in particular an fs_config section, which is the configuration
        for the filesystem watching, will be passed as argument to `file_generator`. The other sections are passed
        further to ``file_publisher_from_generator``.
    """
    fs_config = config["fs_config"]
    logger.info(f"Starting watch on '{fs_config['directory']}'")
    if "password" in fs_config.get("storage_options", []):
        raise SecurityError("A password cannot be published safely.")
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, config)


def file_generator(directory, observer_type="os", file_pattern=None, protocol=None, storage_options=None):
    """Generate new files appearing in the watched directory.

    Args:
        directory: The locally accessible directory to watch for changes.
        observer_type: What to use for detecting changes.
            It can be either "os" for os-based detections (eg inotify on linux, but can be polling on some os's),
            "polling" for detecting changes through polling, or the actual watchdog class to use as observer.
            Defaults to "os".
        file_pattern: The trollsift pattern to use for matching and extracting metadata from the filename.
            This must not include the directory.
        protocol (optional): In case the file has to be advertised with another protocol than "file".
        storage_options: The storage options for the other protocol. Will be ignored if protocol is None.

    Returns:
        A tuple of Path or UPath and file metadata.

    Examples:
        To iterate over new files in `/tmp/`:

        >>> for filename in file_generator("/tmp/", file_pattern="{start_time:%Y%m%d_%H%M}_{product}.tif")
        ...    print(filename)
        Path("/tmp/20200428_1000_foo.tif")


        To get UPaths with ssh as protocol and a specific host:

        >>> for filename in file_generator("/tmp/", file_pattern="{start_time:%Y%m%d_%H%M}_{product}.tif",
        ...                                protocol="ssh", storage_option=dict(host="myhost.pytroll.org"))
        UPath("ssh:///tmp/20200428_1000_foo.tif")  # .storage_options will show the host.

    """
    file_metadata = {}
    pattern = os.path.join(directory, file_pattern) if file_pattern is not None else None
    with listen_to_local_events(directory, file_pattern, observer_type) as events:
        for path in events:
            try:
                file_metadata = parse_metadata(pattern, path)
            except ValueError:
                continue
            if protocol is not None:
                uri = urlunparse((protocol, None, str(path), None, None, None))
                if storage_options is None:
                    storage_options = dict()
                yield UPath(uri, **storage_options), file_metadata
            else:
                yield Path(path), file_metadata
