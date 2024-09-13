"""Tests for the publisher functionality."""

import os
from shutil import make_archive

from posttroll.message import Message
from posttroll.testing import patched_publisher
from upath import UPath

from pytroll_watchers.publisher import file_publisher_from_generator


def test_unpacking(tmp_path):
    """Test unpacking the watched file's components to a dataset message."""
    file1 = tmp_path / "to_zip" / "file1"
    file2 = tmp_path / "to_zip" / "file2"
    zip_file = tmp_path / "archived.zip"

    file1.parent.mkdir()

    open(file1, "a").close()
    open(file2, "a").close()

    make_archive(os.path.splitext(zip_file)[0], "zip", tmp_path / "to_zip")

    path = UPath("file://" + str(zip_file))

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"),
                            unpack="zip")

    with patched_publisher() as messages:
        file_publisher_from_generator([[path, dict()]],
                                      publisher_config=publisher_settings,
                                      message_config=message_settings)

    assert "zip://file1" in messages[0]
    assert "zip://file2" in messages[0]


def test_unpacking_directory(tmp_path):
    """Test unpacking the watched directory's components to a dataset message."""
    file1 = tmp_path / "my_dir" / "file1"
    file2 = tmp_path / "my_dir" / "file2"

    dirpath = file1.parent
    dirpath.mkdir()

    open(file1, "a").close()
    open(file2, "a").close()

    path = UPath("file://" + str(dirpath))

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"),
                            unpack="directory", include_dir_in_uid=True)

    with patched_publisher() as messages:
        file_publisher_from_generator([[path, dict()]],
                                      publisher_config=publisher_settings,
                                      message_config=message_settings)

    assert "my_dir/file1" in messages[0]
    assert "my_dir/file2" in messages[0]
    msg = Message.decode(messages[0])
    assert msg.data["dataset"][0]["uid"].startswith("my_dir")
