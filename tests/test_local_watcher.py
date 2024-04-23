"""Test the local watcher."""

import os

import pytest
from posttroll.message import Message
from posttroll.testing import patched_publisher
from pytroll_watchers import local_watcher
from pytroll_watchers.publisher import SecurityError
from pytroll_watchers.testing import patched_local_events  # noqa


def test_watchdog_generator_with_os(tmp_path, patched_local_events):  # noqa
    """Test a watchdog generator."""
    filename = os.fspath(tmp_path / "20200428_1000_foo.tif")

    with patched_local_events([filename]):
        fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

        generator = local_watcher.file_generator(tmp_path,
                                                "os",
                                                file_pattern=fname_pattern)
    path, metadata = next(generator)

    assert str(path) == filename
    assert metadata["product"] == "foo"


def test_watchdog_generator_with_protocol(tmp_path, patched_local_events):  # noqa
    """Test a watchdog generator."""
    filename = os.fspath(tmp_path / "20200428_1000_foo.tif")

    with patched_local_events([filename]):
        fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

        protocol = "ssh"
        storage_options = {"parameter": "value",
                           "host": "somehost.pytroll.org"}


        generator = local_watcher.file_generator(tmp_path,
                                                 file_pattern=fname_pattern,
                                                 protocol=protocol,
                                                 storage_options=storage_options)
    path, metadata = next(generator)

    assert path.as_uri().startswith("ssh://")
    assert path.as_uri().endswith(filename)
    assert path.protocol == protocol
    assert path.storage_options == storage_options
    assert metadata["product"] == "foo"


def test_watchdog_generator_with_polling(tmp_path, patched_local_events):  # noqa
    """Test a watchdog generator."""
    filename = os.fspath(tmp_path / "20200428_1000_foo.tif")
    with patched_local_events([filename]):
        fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"
        generator = local_watcher.file_generator(tmp_path,
                                                 "polling",
                                                 file_pattern=fname_pattern)

    path, _ = list(generator)[0]
    assert str(path) == filename


def test_watchdog_generator_with_something_else(tmp_path):
    """Test a watchdog generator."""
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

    generator = local_watcher.file_generator(tmp_path,
                                             "something_else",
                                             file_pattern=fname_pattern)

    with pytest.raises(ValueError, match="'os' or 'polling'"):
        next(generator)


def test_publish_paths(tmp_path, patched_local_events, caplog):  # noqa
    """Test publishing paths."""
    filename = os.fspath(tmp_path / "foo.txt")

    local_settings = dict(directory=tmp_path)
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))

    caplog.set_level("INFO")
    with patched_local_events([filename]):
        with patched_publisher() as messages:
            local_watcher.file_publisher(fs_config=local_settings,
                                        publisher_config=publisher_settings,
                                        message_config=message_settings)

    assert "uri" not in message_settings["data"]
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    assert message.data["uri"] == f"file://{str(tmp_path)}/foo.txt"
    assert message.data["sensor"] == "viirs"
    assert "fs" not in message.data
    assert f"Starting watch on '{local_settings['directory']}'" in caplog.text


def test_publish_paths_forbids_passing_password(tmp_path, patched_local_events, caplog):  # noqa
    """Test publishing paths."""
    filename = os.fspath(tmp_path / "foo.txt")
    password = "very strong"  # noqa

    local_settings = dict(directory=tmp_path, protocol="ssh",
                          storage_options=dict(host="myhost.pytroll.org", username="user", password=password))
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))

    caplog.set_level("INFO")
    with patched_local_events([filename]):
        with patched_publisher():
            with pytest.raises(SecurityError):
                local_watcher.file_publisher(fs_config=local_settings,
                                            publisher_config=publisher_settings,
                                            message_config=message_settings)
