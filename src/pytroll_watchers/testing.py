"""Pytest fixtures and utilities for testing code that uses pytroll watchers."""

import datetime
import os
from contextlib import contextmanager, nullcontext

import pytest
import responses
import yaml


@pytest.fixture(autouse=True)
def patched_local_events(monkeypatch):
    """Patch the events produced by underlying os/polling watcher.

    Example:
        The produced context managed can be used like this:

        >>> with patched_local_events(["/tmp/file1", "/tmp/file2"]):
        ...    assert "/tmp/file1" in local_watcher.file_generator("/tmp")

    """
    @contextmanager
    def _patched_local_events(paths):
        def fake_iterator(_):
            return paths
        from pytroll_watchers.backends import local
        monkeypatch.setattr(local, "_iterate_over_queue", fake_iterator)
        yield
    return _patched_local_events


@pytest.fixture(autouse=True)
def patched_bucket_listener(monkeypatch):
    """Patch the records produced by the underlying bucket listener.

    Example:
        This context manager can be used like this:

        >>> with patched_bucket_listener(records_to_produce):
        ...     for record in bucket_notification_watcher.file_generator(endpoint, bucket):
        ...         # do something with the record

    """
    @contextmanager
    def _patched_bucket_listener(records):
        #from unittest import mock
        def fake_listen(*args, **kwargs):
            return nullcontext(enter_result=records)
        import minio
        monkeypatch.setattr(minio.Minio, "listen_bucket_notification", fake_listen)
        yield
    return _patched_bucket_listener


@contextmanager
def load_oauth_responses(*responses_to_load, response_file=None):
    """Load the oauth responses for mocking the requests to copernicus dataspace.

    Args:
        responses_to_load: The responses to load.
        response_file: The file where the responses are stored. Defaults to tests/dataspace_responses.yaml

    Example:
        To get fake response for the watcher and test the generator, one could use::

            with load_oauth_responses("token", "filtered_yesterday"):
                files = list(file_generator(filter_string, check_interval, timedelta(hours=24)))
    """
    today = datetime.datetime.now(datetime.timezone.utc)
    yesterday = today - datetime.timedelta(hours=24)

    if response_file is None:
        response_file = os.path.join("tests", "dataspace_responses.yaml")

    with responses.RequestsMock() as rsps:

        with open(response_file) as fd:
            contents = yaml.safe_load(fd.read())
        for response_to_load in responses_to_load:
            for response in contents["responses"][response_to_load]:
                resp = response["response"]
                resp["url"] = resp["url"].replace("{yesterday}", yesterday.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                resp["url"] = resp["url"].replace("{today}", today.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                rsps.add(**response["response"])

        yield
