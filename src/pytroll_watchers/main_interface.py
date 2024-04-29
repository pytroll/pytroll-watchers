"""Main interface functions."""

import argparse
import logging.config
from importlib.metadata import entry_points

import yaml


def get_publisher_for_backend(backend):
    """Get the right publisher for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_publisher = get_publisher_for_backend("local")
        >>> file_publisher(fs_config, publisher_config, message_config)

    """
    return get_backend(backend).file_publisher


def get_backend(backend):
    """Get the module for a given backend."""
    eps = entry_points(group="pytroll_watchers.backends")
    for ep in eps:
        if backend == ep.name:
            return ep.load()
    raise ValueError(f"Unknown backend {backend}.")


def get_generator_for_backend(backend):
    """Get the right generator for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_generator = get_generator_for_backend("local")
        >>> for filename, file_metadata in file_generator("/tmp"):
        ...     # do something with filename and file_metadata

    """
    return get_backend(backend).file_generator


def publish_from_config(config):
    """Publish files/objects given a config.

    Args:
        config: a dictionary containing the `backend` string (`local` or `minio`), and `fs_config`, `publisher_config`
            and `message_config` dictionaries.
    """
    backend = config["backend"]
    publisher = get_publisher_for_backend(backend)
    return publisher(config["fs_config"], config["publisher_config"], config["message_config"])



def cli(args=None):
    """Command-line interface for pytroll-watchers."""
    parser = argparse.ArgumentParser(
                    prog="pytroll-watcher",
                    description="Watches the appearance of new files/objects on different filesystems.",
                    epilog="Thanks for using pytroll-watchers!")

    parser.add_argument("config", type=str, help="The yaml config file.")
    parser.add_argument("-l", "--log-config", type=str, help="The yaml config file for logging.", default=None)

    parsed = parser.parse_args(args)


    log_config_filename = parsed.log_config
    configure_logging(log_config_filename)

    config_file = parsed.config

    with open(config_file) as fd:
        config_dict = yaml.safe_load(fd.read())

    return publish_from_config(config_dict)


def configure_logging(log_config_filename):
    """Configure logging from a yaml file."""
    if log_config_filename is not None:
        with open(log_config_filename) as fd:
            log_config = yaml.safe_load(fd.read())
    else:
        log_config = {
            "version": 1,
            "formatters": {
                "pytroll": {
                    "format": "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "pytroll",
                },
            },
            "disable_existing_loggers": False,
            "loggers": {
                "": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": True
                },
            },
        }
    logging.config.dictConfig(log_config)
