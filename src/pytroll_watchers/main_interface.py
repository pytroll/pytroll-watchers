"""Main interface functions."""

import argparse

import yaml

from pytroll_watchers.local_watcher import file_generator as local_generator
from pytroll_watchers.local_watcher import file_publisher as local_publisher
from pytroll_watchers.minio_notification_watcher import file_generator as minio_generator
from pytroll_watchers.minio_notification_watcher import file_publisher as minio_publisher


def get_publisher_for_backend(backend):
    """Get the right publisher for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_publisher = get_publisher_for_backend("local")
        >>> file_publisher(fs_config, publisher_config, message_config)

    """
    if backend == "minio":
        return minio_publisher
    elif backend == "local":
        return local_publisher
    else:
        raise ValueError(f"Unknown backend {backend}.")

def get_generator_for_backend(backend):
    """Get the right generator for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_generator = get_generator_for_backend("local")
        >>> for filename, file_metadata in file_generator("/tmp"):
        ...     # do something with filename and file_metadata

    """
    if backend == "minio":
        return minio_generator
    elif backend == "local":
        return local_generator
    else:
        raise ValueError(f"Unknown backend {backend}.")


def publish_from_config(config):
    """Publish files/objects given a config.

    Args:
        config: a dictionary containing the `backend` string (`local` or `minio`), and `fs_config`, `publisher_config`
            and `message_config` dictionaries.
    """
    if config["backend"] == "local":
        return local_publisher(config["fs_config"], config["publisher_config"], config["message_config"])
    elif config["backend"] == "minio":
        return minio_publisher(config["fs_config"], config["publisher_config"], config["message_config"])
    else:
        raise ValueError(f"Unknown backend {config['backend']}")

def cli(args=None):
    """Command-line interface for pytroll-watchers."""
    parser = argparse.ArgumentParser(
                    prog="pytroll-watcher",
                    description="Watches the appearance of new files/objects on different filesystems.",
                    epilog="Thanks for using pytroll-watchers!")

    parser.add_argument("config", type=str, help="The yaml config file.")

    parsed = parser.parse_args(args)

    config_file = parsed.config

    with open(config_file) as fd:
        config_dict = yaml.safe_load(fd.read())

    return publish_from_config(config_dict)
