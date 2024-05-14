"""Functions and classes for performing message selection.

Selection in this context means making sure only one message refering to some file will be published further.

This is useful when multiple source for the same data are sending messages (eg two reception servers for eumetcast) but
only one of each file is needed for further processing.

At the moment, this module makes use of redis as a refined dictionary for keeping track of the received files.

"""

import time
from contextlib import closing, contextmanager
from functools import cache
from subprocess import Popen

import redis
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config


@cache
def _connect_to_redis(**kwargs):
    return redis.Redis(**kwargs)

class TTLDict:
    """A simple dictionary-like object that discards items older than a time-to-live.

    Not thread-safe.
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


def running_selector(selector_config, subscriber_config):
    """Generate selected messages.

    The aim of this generator is to skip messages that are duplicates to already processed messages.
    Duplicate in this context means messages referring to the same file (even if stored in different locations).

    Args:
        selector_config: a dictionary of arguments to pass to the underlying redis instance, see
          https://redis.readthedocs.io/en/stable/connections.html#redis.Redis. You can also provide a ttl as an int
          (seconds) or timedelta instance.
        subscriber_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.subscriber.create_subscriber_from_dict_config`.

    Yields:
        JSON representations of posttroll messages.
    """
    subscriber = create_subscriber_from_dict_config(subscriber_config)

    with closing(subscriber):
        sel = TTLDict(**selector_config)
        for msg in subscriber.recv():
            key = Message.decode(msg).data["uid"]
            try:
                _ = sel[key]
            except KeyError:
                sel[key] = msg
                yield msg


def _run_selector_with_managed_dict_server(selector_config, subscriber_config, publisher_config):
    """Run the selector with a managed ttldict server."""
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    with closing(publisher):
        for msg in running_selector(selector_config, subscriber_config):
            publisher.send(msg)


def run_selector(selector_config, subscriber_config, publisher_config):
    """Run the selector.

    The aim of the selector is to skip messages that refer to already processed files. For example

    The aim of the selector is to skip messages that are duplicates to already published messages.
    Duplicate in this context means messages referring to the same file (even if stored in different locations).

    Messages that refer to new files will be published.

    Args:
        selector_config: a dictionary of arguments to pass to the underlying redis instance, see
          https://redis.readthedocs.io/en/stable/connections.html#redis.Redis. You can also provide a ttl as an int
          (seconds) or timedelta instance.
        subscriber_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.subscriber.create_subscriber_from_dict_config`. The subscribtion is used as a source for
          messages to process.
        publisher_config: a dictionary of arguments to pass to
          :py:func:`~posttroll.publisher.create_publisher_from_dict_config`. This publisher will send the selected
          messages.

    """
    with _running_redis_server(port=selector_config.get("port")):
        _run_selector_with_managed_dict_server(selector_config, subscriber_config, publisher_config)


@contextmanager
def _running_redis_server(port=None):
    command = ["redis-server"]
    if port:
        port = str(int(port))  # using int first here prevents arbitrary strings to be passed to Popen
        command += ["--port", port]
    proc = Popen(command)  # noqa:S603  port is validated
    time.sleep(.25)
    try:
        yield
    finally:
        proc.terminate()
        proc.wait(3)
