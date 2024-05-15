"""Functions and classes for performing message selection.

Selection in this context means making sure only one message refering to some file will be published further.

This is useful when multiple source for the same data are sending messages (eg two reception servers for eumetcast) but
only one of each file is needed for further processing.

To check if two messages refer to the same data, the *uid* metadata of the messages is used.

At the moment, this module makes use of redis as a refined dictionary for keeping track of the received files. Hence a
redis server instance will be started along with some of the functions here, and with the cli.

A command-line script is also made available by this module. It is called ``pytroll-selector``::

    usage: pytroll-selector [-h] [-l LOG_CONFIG] config

    Selects unique messages (based on uid) from multiple sources.

    positional arguments:
      config                The yaml config file.

    options:
      -h, --help            show this help message and exit
      -l LOG_CONFIG, --log-config LOG_CONFIG
                            The yaml config file for logging.

    Thanks for using pytroll-selector!

An example config file to use with this script is the following::

    selector_config:
      ttl: 30
    publisher_config:
      name: hrit_selector
    subscriber_config:
      addresses:
        - tcp://eumetcast_reception_1:9999
        - tcp://eumetcast_reception_2:9999
      nameserver: false
      topics:
        - /1b/hrit-segment/0deg

The different sections are passed straight on to :py:func:`run_selector`, so check it to have more information about
what to pass to it.


"""

import argparse
import logging
import time
from contextlib import closing, contextmanager
from functools import cache
from pathlib import Path
from subprocess import Popen

import redis
import yaml
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config

from pytroll_watchers.main_interface import configure_logging

logger = logging.getLogger(__name__)

@cache
def _connect_to_redis(**kwargs):
    return redis.Redis(**kwargs)


class TTLDict:
    """A simple dictionary-like object that discards items older than a time-to-live.

    Not thread-safe.

    Args:
        ttl: the time to live of the stored items in integer seconds or as a timedelta instance. Cannot be less
            than 1 second.
        redis_params: the keyword arguments to pass to the underlying :py:class:`~redis.Redis` instance.

    """

    def __init__(self, ttl=300, **redis_params):
        """Set up the instance.

        Args:
            ttl: the time to live of the stored items in integer seconds or as a timedelta instance. Cannot be less
              than 1 second.
            redis_params: the keyword arguments to pass to the underlying :py:class:`~redis.Redis` instance.
        """
        self._redis = _connect_to_redis(**redis_params)
        self._ttl = ttl

    def __getitem__(self, key):
        """Get the value corresponding to *key*."""
        return self._redis[key]

    def __setitem__(self, key, value):
        """Set the *value* corresponding to *key*."""
        res = self._redis.get(key)
        if not res:
            self._redis.set(key, value, ex=self._ttl)

    def __contains__(self, key):
        """Check if key is already present."""
        try:
            _ = self[key]
            return True
        except KeyError:
            return False


def running_selector(selector_config, subscriber_config):
    """Generate selected messages.

    The aim of this generator is to skip messages that are duplicates to already processed messages.
    Duplicate in this context means messages referring to the same file (even if stored in different locations).

    Args:
        selector_config: a dictionary of arguments to pass to the underlying redis instance, see
          :py:class:`~redis.Redis`. You can also provide a ttl as an int
          (seconds) or timedelta instance, otherwise it defaults to 300 seconds (5 minutes).
        subscriber_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.subscriber.create_subscriber_from_dict_config`.

    Yields:
        JSON representations of posttroll messages.
    """
    ttl_dict = TTLDict(**selector_config)

    for msg in _data_messages(subscriber_config):
        key = unique_key(msg)
        msg_string = str(msg)

        if key not in ttl_dict:
            ttl_dict[key] = msg_string
            logger.info(f"New content {msg_string}")
            yield msg_string
        else:
            logger.debug(f"Discarded {msg_string}")


def _data_messages(subscriber_config):
    """Generate messages referring to new data from subscriber settings."""
    subscriber = create_subscriber_from_dict_config(subscriber_config)

    with closing(subscriber):
        for msg in subscriber.recv():
            if msg.type != "file":
                continue
            yield msg


def unique_key(msg):
    """Identify the content of the message with a unique key."""
    return msg.data["uid"]


def _run_selector_with_managed_dict_server(selector_config, subscriber_config, publisher_config):
    """Run the selector with a managed ttldict server."""
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    with closing(publisher):
        for msg in running_selector(selector_config, subscriber_config):
            publisher.send(msg)


def run_selector(selector_config, subscriber_config, publisher_config):
    """Run the selector.

    The aim of the selector is to skip messages that are duplicates to already published messages.
    Duplicate in this context means messages referring to the same file (even if stored in different locations).

    Messages that refer to new files will be published.

    Args:
        selector_config: a dictionary of arguments to pass to the underlying redis instance, see
          :py:class:`~redis.Redis`. You can also provide a *ttl* for the
          selector as an int (seconds) or timedelta instance, so that incoming messages are forgotten after that time.
          If not provided, the ttl defaults to 300 seconds (5 minutes).
          Also, you can provide a *directory* for the underlying datastructure to store the data in.
        subscriber_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.subscriber.create_subscriber_from_dict_config`. The subscribtion is used as a source for
          messages to process.
        publisher_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.publisher.create_publisher_from_dict_config`. This publisher will send the selected
          messages.

    """
    with _started_redis_server(port=selector_config.get("port"), directory=selector_config.pop("directory", None)):
        _run_selector_with_managed_dict_server(selector_config, subscriber_config, publisher_config)


@contextmanager
def _started_redis_server(port=None, directory=None):
    command = ["redis-server"]
    if port:
        port = str(int(port))  # using int first here prevents arbitrary strings to be passed to Popen
        command += ["--port", port]
    if directory:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        command += ["--dir", directory]
    proc = Popen(command)  # noqa:S603  port is validated and directory is a Path
    time.sleep(.25)
    try:
        yield
    finally:
        proc.terminate()
        proc.wait(3)


def cli(args=None):
    """Command line interface."""
    parser = argparse.ArgumentParser(
                    prog="pytroll-selector",
                    description="Selects unique messages (based on uid) from multiple sources.",
                    epilog="Thanks for using pytroll-selector!")

    parser.add_argument("config", type=str, help="The yaml config file.")
    parser.add_argument("-l", "--log-config", type=str, help="The yaml config file for logging.", default=None)

    parsed = parser.parse_args(args)


    log_config_filename = parsed.log_config
    configure_logging(log_config_filename)

    config_file = parsed.config

    with open(config_file) as fd:
        config_dict = yaml.safe_load(fd.read())

    selector_config = config_dict.get("selector_config", {})
    subscriber_config = config_dict.get("subscriber_config", {})
    publisher_config = config_dict.get("publisher_config", {})

    return run_selector(selector_config, subscriber_config, publisher_config)
