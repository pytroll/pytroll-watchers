"""Tests for the gathered publisher functions."""

import pytest
from pytroll_watchers import get_generator_for_backend, get_publisher_for_backend
from pytroll_watchers.local_watcher import file_generator as local_generator
from pytroll_watchers.local_watcher import file_publisher as local_publisher
from pytroll_watchers.minio_notification_watcher import file_generator as minio_generator
from pytroll_watchers.minio_notification_watcher import file_publisher as minio_publisher


def test_getting_right_publisher():
    """Test getting the right publisher for a given backend."""
    file_publisher = get_publisher_for_backend("minio")
    assert file_publisher == minio_publisher
    file_publisher = get_publisher_for_backend("local")
    assert file_publisher == local_publisher
    with pytest.raises(ValueError, match="Unknown backend"):
        _ = get_publisher_for_backend("some_other_backend")

def test_getting_right_generator():
    """Test getting the right generator for a given backend."""
    generator = get_generator_for_backend("minio")
    assert generator == minio_generator
    generator = get_generator_for_backend("local")
    assert generator == local_generator
    with pytest.raises(ValueError, match="Unknown backend"):
        _ = get_generator_for_backend("some_other_backend")
