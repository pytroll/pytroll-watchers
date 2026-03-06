"""Tests for the publisher functionality."""

import os
import tarfile
from shutil import make_archive

import pytest
from posttroll.message import Message
from posttroll.testing import patched_publisher
from upath import UPath

from pytroll_watchers.publisher import file_publisher_from_generator


def test_unpacking_raises_warning_when_passed_via_message_config(tmp_path):
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

    with patched_publisher():
        with pytest.deprecated_call(match="data_config"):
            file_publisher_from_generator([[path, dict()]],
                                          dict(publisher_config=publisher_settings,
                                               message_config=message_settings))


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
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"))
    data_config = dict(unpack=dict(format="zip"))

    with patched_publisher() as messages:
        file_publisher_from_generator([[path, dict()]],
                                      dict(publisher_config=publisher_settings,
                                           message_config=message_settings,
                                           data_config=data_config))

    assert "zip://file1" in messages[0]
    assert "zip://file2" in messages[0]


def test_unpacking_tar(tmp_path):
    """Test unpacking the watched file's components to a dataset message."""
    file1 = tmp_path / "to_tar" / "file1"
    file2 = tmp_path / "to_tar" / "file2"
    tar_file = tmp_path / "S3A" / "archived.tar"

    file1.parent.mkdir()
    tar_file.parent.mkdir()

    open(file1, "a").close()
    with open(file2, "a") as fd:
        fd.write("hello")

    make_clean_tar(tmp_path / "to_tar", tar_file)

    path = UPath("file://" + str(tar_file))

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"))
    data_config = dict(unpack=dict(format="tar"))

    with patched_publisher() as messages:
        file_publisher_from_generator([[path, dict()]],
                                      dict(publisher_config=publisher_settings,
                                           message_config=message_settings,
                                           data_config=data_config))

    assert "tar://file1" in messages[0]
    assert "tar://file2" in messages[0]


def test_unpacking_tar_over_sftp(tmp_path):
    """Test that unpacking a tar file represented as an sftp path does not attempt SSH connections.

    The fixture uses a localhost port that refuses connections so any real SSH attempt fails immediately.
    """
    file1 = tmp_path / "to_tar" / "file1.nc"
    file1.parent.mkdir()
    file1.write_text("data")

    tar_file = tmp_path / "archive.tar"
    make_clean_tar(tmp_path / "to_tar", tar_file)

    sftp_path = UPath(f"sftp://127.0.0.1{tar_file}", host="127.0.0.1", port=1)

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"))
    data_config = dict(unpack=dict(format="tar"))

    with patched_publisher() as messages:
        file_publisher_from_generator([[sftp_path, {}]],
                                      dict(publisher_config=publisher_settings,
                                           message_config=message_settings,
                                           data_config=data_config))

    assert len(messages) == 1
    assert "tar://file1.nc" in messages[0]


def make_clean_tar(source_dir, dest_tar):
    """Create a tar archive without the literal './' prefix."""
    with tarfile.open(dest_tar, "w") as tar:
        for root, _, files in os.walk(source_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, source_dir)
                tar.add(full_path, arcname=arcname)


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
    message_settings = dict(subject="/segment/olci/l2/", atype="dataset", data=dict(sensor="olci"))
    data_config = dict(unpack=dict(format="directory", include_dir_in_uid=True))

    with patched_publisher() as messages:
        file_publisher_from_generator([[path, dict()]],
                                      dict(publisher_config=publisher_settings,
                                           message_config=message_settings,
                                           data_config=data_config))

    assert "my_dir/file1" in messages[0]
    assert "my_dir/file2" in messages[0]
    msg = Message.decode(messages[0])
    assert msg.data["dataset"][0]["uid"].startswith("my_dir")


def test_publish_paths_with_fetching(tmp_path):
    """Test publishing paths."""
    basename = "foo+bar,baz_.txt"
    filename = os.fspath(tmp_path / basename)
    with open(filename, "w"):
        pass

    destination = tmp_path / "downloaded"
    destination.mkdir()

    publisher_settings = dict(nameservers=False, port=1979)
    subject = "/hey/jude"
    atype = "atomic"
    message_settings = dict(atype="file", data=dict(sensor="viirs"))
    data_config = dict(fetch=dict(destination=destination))

    items = [(filename, dict(subject=subject, atype=atype,
                             data=dict(mime="txt")))]

    with patched_publisher() as messages:
        config = dict(publisher_config=publisher_settings,
                      message_config=message_settings,
                      data_config=data_config)
        file_publisher_from_generator(items, config)

    assert "uri" not in message_settings["data"]
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    assert message.subject == subject
    assert message.type == "file"
    assert message.data["sensor"] == "viirs"
