"""Common functions for publishing messages."""

from contextlib import closing, suppress
from copy import deepcopy

from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config


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

            amended_message_config["data"].update(file_metadata)
            msg = Message(**amended_message_config)
            publisher.send(str(msg))
