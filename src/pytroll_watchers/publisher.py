"""Common functions for publishing messages."""

import datetime
from contextlib import closing, suppress
from copy import deepcopy

from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from trollsift import parse


def file_publisher_from_generator(generator, publisher_config, message_config):
    """Publish files coming from local filesystem events.

    Args:
        generator: the generator to use for producing files. The generator must yield tuples of
            (filename, file_metadata).
        publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
        message_config: The information needed to complete the posttroll message generation. Will be amended
             with the file metadata, and passed directly to posttroll's Message constructor.
    """
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    with closing(publisher):
        for file_item, file_metadata in generator:
            amended_message_config = deepcopy(message_config)
            amended_message_config["data"]["uri"] = file_item.as_uri()
            with suppress(AttributeError):
                amended_message_config["data"]["fs"] = file_item.fs.to_json()
            aliases = amended_message_config.pop("aliases", {})
            apply_aliases(aliases, file_metadata)
            amended_message_config["data"].update(file_metadata)
            msg = Message(**amended_message_config)
            publisher.send(str(msg))


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
