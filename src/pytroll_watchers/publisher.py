"""Common functions for publishing messages."""

import datetime
import json
import logging
from collections.abc import Generator
from contextlib import closing, contextmanager, suppress
from copy import deepcopy
from typing import Any
from urllib.parse import unquote
from warnings import warn

import fsspec
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from trollsift import parse
from upath import UPath

from pytroll_watchers.fetch import fetch_file

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    """An exception for breaking security rules."""


def file_publisher_from_generator(generator: Generator[tuple[UPath, dict[str, Any]]],
                                  config: dict[str, Any]) -> None:
    """Publish files coming from local filesystem events.

    Args:
        generator: the generator to use for producing files. The generator must yield tuples of
            (filename, file_metadata).
        config: the configuration containing the parameters to use for publishing data. It should contain the following
                sections:
                  - publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
                  - message_config: The information needed to complete the posttroll message generation. Will be amended
                      with the file metadata, and passed directly to posttroll's Message constructor.
                  - data_config: The information about the processing to do on the uris before sending.
                      The `fetch` section, if present, will trigger the fetching of the file locally before sendig the
                      (adjusted) uri further. The parameter `destination` should be provided as the directory to put
                      the downloaded files in.
                      The `unpack` section that contains the packing `format` for the archive (eg "zip"), or
                      "directory". The contents of the archive or directory will be published as a "dataset". For the case where
                      "directory" is used, it is also possible to set the boolean "include_dir_in_uid" to true so that
                      the full relative path of the file is provided (False by default).

    Side effect:
        Publishes posttroll messages containing the location of the file with the following fields:
          - The "uid" which is the unique filename of the file.
          - The "uri" which provides an fsspec-style uri to the file. This does not however contain the connection
            parameters, or storage options of the filesystems referred to.
          - The "filesystem" which is the json-serialized filesystem information that can then be fed to fsspec.
          - The "path" which is the path of the file inside the filesystem.

        For example, the file
          - uid: `S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiance.nc`
          - uri: `s3:///eodata/Sentinel-3/OLCI/OL_1_EFR___/2024/04/15/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiance.nc`
          - filesystem: `{"cls": "s3fs.core.S3FileSystem", "protocol": "s3", "args": [], "profile": "my_profile"}`
          - path: `/eodata/Sentinel-3/OLCI/OL_1_EFR___/2024/04/15/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3/Oa02_radiance.nc`
    """  # noqa
    publisher_config = config.pop("publisher_config")
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()


    with closing(publisher):
        for file_item, file_metadata in generator:
            msg = _create_message(file_item, file_metadata, config)
            logger.info(f"Sending {str(msg)}")
            publisher.send(str(msg))


def _create_message(file_item, file_metadata, config):
    config = deepcopy(config)
    message_config = config.pop("message_config", dict())
    unpack = message_config.pop("unpack", None)
    if unpack is not None:
        warn("The `unpack` option should be passed inside the `data_config` section", DeprecationWarning, stacklevel=1)

    data_config = config.pop("data_config", dict())

    if file_metadata and ("data" in file_metadata):
        file_mda = deepcopy(file_metadata)
        message_data = file_mda.pop("data")
        message_parameters = file_mda
    else:
        message_data = file_metadata or dict()
        message_parameters = dict()
    message_parameters.update(message_config)
    message_parameters.setdefault("data", {})

    file_location_info = prepare_data(file_item, data_config)
    message_parameters["data"].update(file_location_info)

    aliases = message_parameters.pop("aliases", {})
    apply_aliases(aliases, message_data)

    message_parameters["data"].update(message_data)

    return Message(**message_parameters)


def prepare_data(file_item, data_config):
    """Prepare the data for further processing."""
    fetch = data_config.pop("fetch", {})
    if fetch:
        file_item = fetch_file(file_item, fetch["destination"])

    unpack_info = data_config.pop("unpack", {})
    unpack = unpack_info.get("format", None)

    include_dir = unpack_info.get("include_dir_in_uid", False)

    metadata = dict()
    if unpack == "directory":
        dir_to_include = file_item.name if include_dir else None
        dataset = [_build_file_location(unpacked_file, dir_to_include)
            for unpacked_file in unpack_dir(file_item)]
        metadata["dataset"] = dataset
    elif unpack:
        dataset = [_build_file_location(unpacked_file)
            for unpacked_file in unpack_archive(file_item, unpack)]
        metadata["dataset"] = dataset
    else:
        file_location = _build_file_location(file_item)
        metadata.update(file_location)
    return metadata


def unpack_archive(path, unpack):
    """Unpack the path and yield the extracted filenames."""
    fs = fsspec.get_filesystem_class(unpack)(fsspec.open(path.path, **path.storage_options))
    files = fs.find("/")
    for fi in files:
        yield UPath(fi,
                    protocol=unpack,
                    target_protocol=path.protocol,
                    target_options=path.storage_options,
                    fo=as_uri(path))


def unpack_dir(path):
    """Unpack the directory and generate the files it contains (recursively)."""
    files = path.fs.find(path.path)
    for fi in files:
        yield UPath(fi,
                    protocol=path.protocol,
                    **path.storage_options)


def _build_file_location(file_item, include_dir=None):
    file_location = dict()
    try:
        with dummy_connect(file_item):
            file_location["filesystem"] = json.loads(file_item.fs.to_json(include_password=False))

        file_location["uri"] = as_uri(file_item)
        file_location["path"] = file_item.path
    except AttributeError:  # fileitem is not a UPath if it cannot access .fs
        file_location["uri"] = str(file_item)

    if include_dir:
        uid = include_dir + file_item.path.rsplit(include_dir, 1)[-1]
    else:
        uid = file_item.name
    file_location["uid"] = uid
    return file_location


def as_uri(file_item):
    """Represent file item’s path as an unquoted uri."""
    with suppress(AttributeError):
        protocol = file_item.protocol
        if protocol.startswith("http"):
            return file_item.as_uri()
    return unquote(file_item.as_uri())


@contextmanager
def dummy_connect(file_item):
    """Make the _connect method of the fsspec class a no-op.

    This is for the case where only serialization of the filesystem is needed.
    """
    def _fake_connect(*_args, **_kwargs): ...

    klass = fsspec.get_filesystem_class(file_item.protocol)
    try:
        original_connect = klass._connect
    except AttributeError:
        yield
        return

    klass._connect = _fake_connect
    try:
        yield
    finally:
        klass._connect = original_connect


def apply_aliases(aliases, metadata):
    """Apply aliases to the metadata.

    Args:
        aliases: a dict containing dicts for each key to be aliases. For example
            `{"platform_name": {"npp": "Suomi-NPP}}` will replace the `platform_name` "npp" with "Suomi-NPP".
        metadata: the metadata to fix
    """
    for key, val in metadata.items():
        if key in aliases:
            metadata[key] = aliases[key].get(val, val)


def fix_times(info):
    """Fix times so that date and time components are combined, and start time is before end time."""
    if "start_date" in info:
        info["start_time"] = datetime.datetime.combine(info["start_date"].date(),
                                                       info["start_time"].time())
        if "end_date" not in info:
            info["end_date"] = info["start_date"]
        del info["start_date"]
    if "end_date" in info:
        info["end_time"] = datetime.datetime.combine(info["end_date"].date(),
                                                     info["end_time"].time())
        del info["end_date"]
    if "end_time" in info:
        while info["start_time"] > info["end_time"]:
            info["end_time"] += datetime.timedelta(days=1)

def parse_metadata(file_pattern, path):
    """Parse metadata from the filename."""
    if file_pattern is not None:
        file_metadata = parse(file_pattern, path)
        fix_times(file_metadata)
    else:
        file_metadata = {}
    return file_metadata
