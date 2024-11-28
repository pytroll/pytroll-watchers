"""Common functions for publishing messages."""

import datetime
import json
import logging
from contextlib import closing, contextmanager, suppress
from copy import deepcopy

import fsspec
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from trollsift import parse
from upath import UPath

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    """An exception for breaking security rules."""


def file_publisher_from_generator(generator, publisher_config, message_config):
    """Publish files coming from local filesystem events.

    Args:
        generator: the generator to use for producing files. The generator must yield tuples of
            (filename, file_metadata).
        publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
        message_config: The information needed to complete the posttroll message generation. Will be amended
            with the file metadata, and passed directly to posttroll's Message constructor.
            If it contains "unpack", it is expected to have the archive type (eg "zip"), or "directory", and the
            contents of the archive or directory will be published as a "dataset". For the case where "directory" is
            used, it is also possible to set the boolean "include_dir_in_uid" to true so that the full relative path
            of the file is provided.

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
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    unpack = message_config.pop("unpack", None)
    include_dir = message_config.pop("include_dir_in_uid", None)
    with closing(publisher):
        for file_item, file_metadata in generator:
            amended_message_config = deepcopy(message_config)
            amended_message_config.setdefault("data", {})
            if unpack == "directory":
                dir_to_include = file_item.name if include_dir else None
                dataset = [_build_file_location(unpacked_file, dir_to_include)
                           for unpacked_file in unpack_dir(file_item)]
                amended_message_config["data"]["dataset"] = dataset
            elif unpack:
                dataset = [_build_file_location(unpacked_file) for unpacked_file in unpack_archive(file_item, unpack)]
                amended_message_config["data"]["dataset"] = dataset
            else:
                file_location = _build_file_location(file_item)
                amended_message_config["data"].update(file_location)

            aliases = amended_message_config.pop("aliases", {})
            apply_aliases(aliases, file_metadata)
            amended_message_config["data"].update(file_metadata)
            msg = Message(**amended_message_config)
            logger.info(f"Sending {str(msg)}")
            publisher.send(str(msg))


def unpack_archive(path, unpack):
    """Unpack the path and yield the extracted filenames."""
    import fsspec
    fs = fsspec.get_filesystem_class(unpack)(fsspec.open(path.path, **path.storage_options))
    files = fs.find("/")
    for fi in files:
        yield UPath(fi,
                    protocol=unpack,
                    target_protocol=path.protocol,
                    target_options=path.storage_options,
                    fo=path.as_uri())


def unpack_dir(path):
    """Unpack the directory and generate the files it contains (recursively)."""
    files = path.fs.find(path.path)
    for fi in files:
        yield UPath(fi,
                    protocol=path.protocol,
                    **path.storage_options)


def _build_file_location(file_item, include_dir=None):
    file_location = dict()
    file_location["uri"] = file_item.as_uri()
    if include_dir:
        uid = include_dir + file_item.path.rsplit(include_dir, 1)[-1]
    else:
        uid = file_item.name
    file_location["uid"] = uid
    with suppress(AttributeError):  # fileitem is not a UPath if it cannot access .fs
        with dummy_connect(file_item):
            file_location["filesystem"] = json.loads(file_item.fs.to_json(include_password=False))

        file_location["path"] = file_item.path

    return file_location


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
