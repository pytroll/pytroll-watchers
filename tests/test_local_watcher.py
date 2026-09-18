"""Test the local watcher."""

import os
import threading
import time

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


def _start_fetching_next(generator):
    """Start fetching the next item of a generator in the background.

    Returns a function waiting at most `timeout` seconds for that item, re-raising what the generator raised.
    The fetching thread is a daemon, so a generator that never yields cannot hang the test session.
    """
    outcome = {}

    def fetch_next():
        try:
            outcome["item"] = next(generator)
        except Exception as err:
            outcome["error"] = err

    fetcher = threading.Thread(target=fetch_next, daemon=True)
    fetcher.start()

    def wait_for_next(timeout):
        fetcher.join(timeout)
        if fetcher.is_alive():
            raise TimeoutError(f"No item from the generator within {timeout} seconds")
        if "error" in outcome:
            raise outcome["error"]
        return outcome["item"]

    return wait_for_next


def test_os_watcher_yields_files_still_being_written_when_triggering_on_creation(tmp_path):
    """Test the os watcher announces a file as soon as it is created, before it is closed."""
    filename = tmp_path / "20200428_1000_foo.tif"
    generator = local_watcher.file_generator(tmp_path, "os",
                                             file_pattern="{start_time:%Y%m%d_%H%M}_{product}.tif",
                                             trigger="created")

    wait_for_next_file = _start_fetching_next(generator)
    time.sleep(0.2)
    with open(filename, "w"):
        path, _ = wait_for_next_file(timeout=2)

    assert path == filename


def test_watchdog_generator_with_list_of_patterns(tmp_path, patched_local_events):  # noqa
    """Test a watchdog generator."""
    filename1 = os.fspath(tmp_path / "20200428_1000_foo.tif")
    filename2 = os.fspath(tmp_path / "bla_31060.nc")

    with patched_local_events([filename1, filename2]):
        fname_pattern = ["{start_time:%Y%m%d_%H%M}_{product}.tif",
                         "{product}_{orbit:5d}.nc",
                         ]

        generator = local_watcher.file_generator(tmp_path,
                                                "os",
                                                file_pattern=fname_pattern)
    path, metadata = next(generator)

    assert str(path) == filename1
    assert metadata["product"] == "foo"

    path, metadata = next(generator)

    assert str(path) == filename2
    assert metadata["product"] == "bla"

@pytest.mark.timeout(2)
def test_pattern_can_include_dir(tmp_path, patched_local_events):  # noqa
    """Test the local watcher can have a directory included in the pattern."""
    basedir1 = tmp_path / "s3a"
    basedir1.mkdir()
    basedir2 = tmp_path / "s3b"
    basedir2.mkdir()
    filename1 = str(basedir1 / "20200428_1000_foo3a.tif")
    filename2 = str(basedir2 / "20200428_1000_foo3b.tif")

    fname_pattern = "s3{satnum}/{start_time:%Y%m%d_%H%M}_{product}.tif"

    with patched_local_events([filename1, filename2]):
        from pytroll_watchers.backends.local import listen_to_local_events
        with listen_to_local_events(tmp_path, fname_pattern) as event_generator:
            path, _ = next(event_generator)
            assert path == filename1

            path, _ = next(event_generator)
            assert path == filename2


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


def test_watchdog_generator_rejects_unknown_trigger(tmp_path):
    """Test the watcher refuses a trigger it does not know, instead of silently falling back to another one."""
    generator = local_watcher.file_generator(tmp_path, "os", trigger="opened")

    wait_for_next_file = _start_fetching_next(generator)

    with pytest.raises(ValueError, match="'created' or 'closed'"):
        wait_for_next_file(timeout=2)


def test_publish_paths(tmp_path, patched_local_events, caplog):  # noqa
    """Test publishing paths."""
    basename = "foo+bar,baz_.txt"
    filename = os.fspath(tmp_path / basename)

    local_settings = dict(directory=tmp_path)
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))

    caplog.set_level("INFO")
    with patched_local_events([filename]):
        with patched_publisher() as messages:
            local_watcher.file_publisher(dict(fs_config=local_settings,
                                              publisher_config=publisher_settings,
                                              message_config=message_settings))

    assert "uri" not in message_settings["data"]
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    assert message.data["uri"] == f"{str(tmp_path)}/{basename}"
    assert message.data["sensor"] == "viirs"
    assert "filesystem" not in message.data
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
                local_watcher.file_publisher(dict(fs_config=local_settings,
                                                  publisher_config=publisher_settings,
                                                  message_config=message_settings))


def test_publish_paths_with_ssh(tmp_path, patched_local_events):  # noqa
    """Test publishing paths with an ssh protocol."""
    filename = os.fspath(tmp_path / "foo.txt")

    host = "localhost"

    local_settings = dict(directory=tmp_path, protocol="ssh",
                          storage_options=dict(host=host))
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))

    with patched_local_events([filename]):
        with patched_publisher() as published_messages:
            local_watcher.file_publisher(dict(fs_config=local_settings,
                                              publisher_config=publisher_settings,
                                              message_config=message_settings))
            assert len(published_messages) == 1
            message = Message(rawstr=published_messages[0])
            assert message.data["uri"].startswith("ssh://")
            assert message.data["filesystem"]["host"] == host


def test_publish_paths_with_file(tmp_path, patched_local_events):  # noqa
    """Test publishing paths with a file protocol."""
    filename = os.fspath(tmp_path / "foo.txt")

    local_settings = dict(directory=tmp_path, protocol="file")
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))

    with patched_local_events([filename]):
        with patched_publisher() as published_messages:
            local_watcher.file_publisher(dict(fs_config=local_settings,
                                              publisher_config=publisher_settings,
                                              message_config=message_settings))
            assert len(published_messages) == 1
            message = Message(rawstr=published_messages[0])
            assert message.data["uri"].startswith("file://")
            assert "filesystem" in message.data
