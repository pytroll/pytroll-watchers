"""Utilities for testing code that uses pytroll watchers."""

from contextlib import contextmanager, nullcontext

import pytest


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


@contextmanager
def patched_bucket_listener(records):
    """Patch the records produced by the underlying bucket listener.

    Example:
        This context manager can be used like this:

        >>> with patched_bucket_listener(records_to_produce):
        ...     for record in bucket_notification_watcher.file_generator(endpoint, bucket):
        ...         # do something with the record

    """
    from unittest import mock

    import minio
    fake_minio = mock.Mock(wraps=minio.Minio)
    fake_minio.return_value.listen_bucket_notification.return_value = nullcontext(enter_result=records)
    with mock.patch("minio.Minio", fake_minio):
        yield
