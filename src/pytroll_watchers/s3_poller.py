"""Poller for S3 object stores.

Configuration example:

.. code-block:: yaml

    backend: s3
    fs_config:
      bucket_name: sat/L1B
      file_pattern: "SAT_{platform_name}-{start_time:%Y%m%d%H%M%S}_{end_time:%Y%m%d%H%M%S}.nc"
      storage_options:
        profile: sat-store
      polling_interval:
        seconds: 10
      start_from:
        days: 1
    publisher_config:
      name: sat_watcher
      nameservers: false
      port: 3000
    message_config:
      subject: /segment/s1/l1b/
      atype: file
    data_config:
      include_dir_in_uid: true

in this example, the credentials and endpoint are provided through a profile that needs to be defined in the .aws files,
for example, in `.aws/credentials`::

    ...
    [sat-store]
    aws_access_key_id=...
    aws_secret_access_key=...

and in `.aws/config`::

    ...
    [profile sat-store]
    services = sat-store-s3

    [services sat-store-s3]
    S3 =
      endpoint_url = https://sat.store.org


"""
import logging
import os
from collections import deque
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any

import s3fs
from trollsift import Parser
from upath import UPath

from pytroll_watchers.common import run_every
from pytroll_watchers.publisher import file_publisher_from_generator

logger = logging.getLogger(__name__)



def file_publisher(config: dict[str, Any]):
    """Publish files coming from local filesystem events.

    Args:
        config: the configuration dictionary, containing in particular an fs_config section, which is the configuration
        for the filesystem watching, will be passed as argument to `file_generator`. The other sections are passed
        further to ``file_publisher_from_generator``.
    """
    fs_config = config["fs_config"]
    logger.info(f"Starting polling on s3 for '{fs_config['bucket_name']}'")
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, config)


def file_generator(bucket_name: str,
                   polling_interval: timedelta | dict[str, float],
                   file_pattern: str | None = None,
                   start_from: timedelta | dict[str, float] | None = None,
                   storage_options: None | dict[str, bool | int | str]=None) -> Generator[tuple[UPath, dict[str, Any]]]:
    """Generate file UPaths and metadata for a given bucket by polling."""
    if isinstance(polling_interval, dict):
        polling_interval = timedelta(**polling_interval)

    if isinstance(start_from, dict):
        start_from_delta = timedelta(**start_from)
    else:
        start_from_delta: timedelta = start_from or timedelta(0)

    filename_cache: deque[UPath] = deque(maxlen=100)

    last_pub_date: datetime = datetime.now(timezone.utc) - start_from_delta
    for next_check in run_every(polling_interval):
        new_pub_date =  None
        for f, mda in generate_download_links_since(bucket_name, file_pattern, last_pub_date, storage_options):
            if f in filename_cache:
                continue
            new_pub_date = mda["LastModified"]
            filename_cache.append(f)
            yield f, mda
        logger.info("Finished polling.")
        if next_check > datetime.now(timezone.utc):
            logger.info(f"Next iteration at {next_check}")
        last_pub_date = new_pub_date or last_pub_date


def generate_download_links(bucket_name: str,
                            file_pattern: str | None = None,
                            storage_options: dict[str, Any] | None = None) -> Generator[tuple[UPath, dict[str, Any]]]:
    """Generate download links."""
    return generate_download_links_since(bucket_name, file_pattern, None, storage_options)

def generate_download_links_since(bucket_name: str,
                                  file_pattern: str | None = None,
                                  start_from: datetime | None = None,
                                  storage_options: dict[str, Any] | None = None):
    """Generate download links since date."""
    for f, mda in sorted(_poll_files(bucket_name, file_pattern, start_from,**(storage_options or dict())).items(),
                         key=(lambda x: x[1]["LastModified"])):
        s3path = UPath(f, protocol="s3", **(storage_options or dict()))
        yield s3path, mda

def _poll_files(bucket_name: str,
               file_pattern: str | None = None,
               start_date: datetime | None = None,
               **storage_options: Any,
               ) -> dict[str, dict[str, str | datetime | list[str] | int]]:
    """Poll files from s3."""
    s3 = s3fs.S3FileSystem(skip_instance_cache=True, **storage_options)
    if file_pattern:
        parser = Parser(file_pattern)
        file_pattern = parser.globify()
    flist = s3.glob(bucket_name + "/" + (file_pattern or "*"), detail=True)
    if start_date:
        flist = {f: mda for f, mda in flist.items() if mda["LastModified"] >= start_date}
    if file_pattern:
        for f, mda in flist.items():
            mda.update(parser.parse(os.path.basename(f)))
    return flist
