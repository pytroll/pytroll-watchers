"""Tests for the gathered publisher functions."""

import logging

import pytest
import yaml
from posttroll.testing import patched_publisher
from pytroll_watchers.local_watcher import file_generator as local_generator
from pytroll_watchers.local_watcher import file_publisher as local_publisher
from pytroll_watchers.main_interface import (
    cli,
    get_generator_for_backend,
    get_publisher_for_backend,
    publish_from_config,
)
from pytroll_watchers.minio_notification_watcher import file_generator as minio_generator
from pytroll_watchers.minio_notification_watcher import file_publisher as minio_publisher
from pytroll_watchers.testing import patched_bucket_listener, patched_local_events  # noqa


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


def test_pass_config_to_file_publisher_for_local_backend(tmp_path, patched_local_events):  # noqa
    """Test passing a config to create a file publisher from a local fs."""
    local_settings = dict(directory=tmp_path)
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))
    config = dict(backend="local",
                  fs_config=local_settings,
                  publisher_config=publisher_settings,
                  message_config=message_settings)
    with patched_publisher() as msgs:
        filename = tmp_path / "bla"
        with patched_local_events([filename]):
            publish_from_config(config)
            assert len(msgs) == 1
            assert str(filename) in msgs[0]


def test_pass_config_to_object_publisher_for_minio_backend(patched_bucket_listener):  # noqa
    """Test passing a config to create an objec publisher from minio bucket."""
    s3_settings = dict(endpoint_url="someendpoint",
                       bucket_name="viirs-data",
                       storage_options=dict())
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))
    config = dict(backend="minio",
                  fs_config=s3_settings,
                  publisher_config=publisher_settings,
                  message_config=message_settings)

    records = [{"Records": [{
               "eventName": "s3:ObjectCreated:Put",
               "s3": {"bucket": {"arn": "arn:aws:s3:::viirs-data",
                                 "name": "viirs-data",
                                 "ownerIdentity": {"principalId": "someuser"}},
                      "object": {"contentType": "application/x-hdf5",
                                 "key": "sdr/bla.h5",
                                 "size": 22183568,
                                 "userMetadata": {"content-type": "application/x-hdf5"}}}}]}]


    with patched_publisher() as msgs:
        with patched_bucket_listener(records):
            publish_from_config(config)
            assert len(msgs) == 1
            assert str("sdr/bla.h5") in msgs[0]


def test_pass_config_to_file_publisher_for_spurious_backend():
    """Test that spurious backend fails."""
    config = {}
    config["backend"] = "some_other_backend"
    with pytest.raises(ValueError, match="Unknown backend"):
        publish_from_config(config)


def test_cli(tmp_path, patched_local_events):  # noqa
    """Test the command-line interface."""
    local_settings = dict(directory=str(tmp_path))
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))
    config = dict(backend="local",
                  fs_config=local_settings,
                  publisher_config=publisher_settings,
                  message_config=message_settings)

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as fd:
        fd.write(yaml.dump(config))

    with patched_publisher() as msgs:
        filename = tmp_path / "bla"
        with patched_local_events([filename]):
            cli([str(config_file)])
            assert len(msgs) == 1
            assert str(filename) in msgs[0]


def test_cli_with_logging(tmp_path, patched_local_events):  # noqa
    """Test the command-line interface with logging."""
    local_settings = dict(directory=str(tmp_path))
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))
    config = dict(backend="local",
                  fs_config=local_settings,
                  publisher_config=publisher_settings,
                  message_config=message_settings)

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as fd:
        fd.write(yaml.dump(config))

    log_config_file = tmp_path / "log_config.yaml"
    handler_name = "console123"
    log_config = {
        "version": 1,
        "handlers": {
            handler_name: {
                "class": "logging.StreamHandler",
                "level": "INFO",
            },
        },
        "loggers": {
            "": {
                "level": "INFO",
                "handlers": [handler_name],
            },
        },
    }
    with open(log_config_file, "w") as fd:
        fd.write(yaml.dump(log_config))

    with patched_publisher():
        filename = tmp_path / "bla"
        with patched_local_events([filename]):
            cli([str(config_file), "-l", str(log_config_file)])
            root = logging.getLogger()
            assert len(root.handlers) == 1
            assert root.handlers[0].name == handler_name
