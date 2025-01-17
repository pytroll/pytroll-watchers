"""Tests for the fetching capability."""

import json
from urllib.parse import quote
from zipfile import ZipFile

import fsspec

from pytroll_watchers.fetch import fetch_file


def create_data_file(path):
    """Create a data file."""
    path.parent.mkdir()

    with open(path, "w") as fd:
        fd.write("data")


def test_fetcher_with_simple_uri(tmp_path):
    """Test fetcher with a simple uri."""
    filename = "important_data.nc"
    test_file = tmp_path / filename
    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    with open(test_file, "w") as fd:
        fd.write("very important data.")
    uri = test_file.as_uri()
    fetch_file(uri, str(download_dir))
    assert (download_dir / filename).exists()


# def test_fetcher_with_decompression(tmp_path):
#     """Test fetcher with a simple uri."""
#     filename = "important_data.txt.bz2"
#     test_file = tmp_path / filename
#     download_dir = tmp_path / "downloaded"
#     download_dir.mkdir()
#     data = b"very important data."
#     with open(test_file, "wb") as fd:
#         fd.write(bz2.compress(data))
#     from zipfile import ZipFile
#     zfile = str(test_file) + ".zip"
#     with ZipFile(zfile, "w") as zipf:
#         zipf.write(test_file, arcname=filename)
#     of = fsspec.open("zip://" + filename + "::file://" + zfile)
#
#     fs = json.loads(of.fs.to_json())
#     fetch_file(of.path, download_dir, fs, compression="infer")
#     assert (download_dir / filename).open("rb").read() == data
#     # TODO
#     # assert str(download_dir / filename).endswith(".txt")
#
#
# def test_fetcher_with_different_dest_filename(tmp_path):
#     """Test fetcher with a simple uri."""
#     filename = "important_data.nc"
#     test_file = tmp_path / filename
#     download_dir = tmp_path / "downloaded"
#     download_dir.mkdir()
#     with open(test_file, "w") as fd:
#         fd.write("very important data.")
#     source_pattern = "{name}.nc"
#     destination_filename = "very_{name}.txt"
#     uri = test_file.as_uri()
#     new_name = construct_new_name(uri, source_pattern, destination_filename)
#     fetch_file(uri, new_name)
#     assert (download_dir / ("very_" + filename[:-2] + "txt")).exists()


def test_fetcher_with_complex_uri(tmp_path):
    """Test fetcher with a complex uri."""
    filename = "important_data.nc"
    test_file = tmp_path / filename
    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    with open(test_file, "w") as fd:
        fd.write("very important data.")
    uri = "simplecache::" + test_file.as_uri()
    fetch_file(uri, download_dir)
    assert (download_dir / filename).exists()


def test_fetcher_with_annoying_filename(tmp_path):
    """Test fetcher with a filename containing symbols."""
    filename = "important+data,here.nc"
    test_file = tmp_path / filename
    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    with open(quote(str(test_file)), "w") as fd:
        fd.write("very important data.")
    uri = test_file.as_uri()
    fetch_file(uri, str(download_dir))
    assert (download_dir / filename).exists()


def test_fetcher_with_fs(tmp_path):
    """Test fetcher with a filesystem."""
    filename = "important_data.nc"
    test_file = tmp_path / filename
    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    with open(test_file, "w") as fd:
        fd.write("very important data.")
    uri = test_file.as_uri()
    fs = {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}
    fetch_file(uri, download_dir, fs)
    assert (download_dir / filename).exists()


def test_fetcher_with_complex_uri_and_fs(tmp_path):
    """Test fetcher with a complex uri and a filesystem."""
    filename = "important_data.nc"
    test_file = tmp_path / filename

    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()

    with open(test_file, "w") as fd:
        fd.write("very important data.")

    compressed_test_file = tmp_path / "important.zip"
    with ZipFile(compressed_test_file, "w") as myzip:
        myzip.write(test_file)

    uri = "zip://" + str(test_file) + "::" + compressed_test_file.as_uri()
    fs = json.loads(fsspec.open(uri).fs.to_json())

    returned_filename = fetch_file(str(test_file), download_dir, fs)
    downloaded_filename = download_dir / filename
    assert downloaded_filename.exists()
    assert downloaded_filename == returned_filename


def test_fetcher_with_complex_uri_and_fs_2(tmp_path):
    """Test fetcher with a complex uri and a filesystem."""
    filename = "important_data.nc"
    test_file = tmp_path / filename

    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()

    with open(test_file, "w") as fd:
        fd.write("very important data.")

    compressed_test_file = tmp_path / "important.zip"
    with ZipFile(compressed_test_file, "w") as myzip:
        myzip.write(test_file)

    uri = "zip://" + str(test_file) + "::" + compressed_test_file.as_uri()
    fs = json.loads(fsspec.open(uri).fs.to_json())

    returned_filename = fetch_file("zip://" + str(test_file), download_dir, fs)
    downloaded_filename = download_dir / filename
    assert downloaded_filename.exists()
    assert downloaded_filename == returned_filename
