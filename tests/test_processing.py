"""Test intermediate steps like fetching and unpacking."""

import os

from posttroll.message import Message
from posttroll.testing import patched_publisher

from pytroll_watchers import local_watcher
from pytroll_watchers.testing import patched_local_events  # noqa


def test_publish_paths_with_fetching(tmp_path, patched_local_events):  # noqa
    """Test publishing paths."""
    basename = "foo+bar,baz_.txt"
    filename = os.fspath(tmp_path / basename)
    with open(filename, "w"):
        pass

    destination = tmp_path / "downloaded"
    destination.mkdir()

    local_settings = dict(directory=tmp_path)
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/viirs/l1b/", atype="file", data=dict(sensor="viirs"))
    data_config = dict(fetch=dict(destination=destination))

    with patched_local_events([filename]):
        with patched_publisher() as messages:
            local_watcher.file_publisher(dict(fs_config=local_settings,
                                              publisher_config=publisher_settings,
                                              message_config=message_settings,
                                              data_config=data_config))

    assert "uri" not in message_settings["data"]
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    assert message.data["uri"] == f"{str(destination)}/{basename}"
    assert message.data["sensor"] == "viirs"
    assert "filesystem" not in message.data
