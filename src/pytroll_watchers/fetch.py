"""Fetch files from other (remote) filesystems."""

import json
import logging
import os
from pathlib import Path
from urllib.parse import unquote

import fsspec

logger = logging.getLogger(__name__)


def fetch_file(file_to_fetch, download_dir, filesystem=None):
    """Fetch a file.

    Args:
        file_to_fetch: The Path of the file to fetch. Can be a UPath from universal_path.
        download_dir: The directory to download the file to.
        filesystem: The file system to use if provided. Should be a dictionary that will be fed to fsspec.

    Returns:
        The Path to the downloaded file.

    Example:

        >>> fetch_file("https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-FLDK/2017/02/02/0020/HS_H08_20170202_0020_B01_FLDK_R10_S0101.DAT.bz2", "/tmp/")
        ... # HS_H08_20170202_0020_B01_FLDK_R10_S0101.DAT.bz2 is now present in /tmp/


    """  # noqa
    download_dir = Path(download_dir)

    if filesystem:
        downloaded_file = _fetch_from_json_filesystem(file_to_fetch, download_dir, filesystem)
    else:
        downloaded_file = _fetch_from_uri(file_to_fetch, download_dir)
    logger.info(f"Fetched {str(downloaded_file)}")
    return downloaded_file


def _fetch_from_uri(file_to_fetch, download_dir):
    """Fetch a file from a uri."""
    fs_file = fsspec.open(file_to_fetch)
    filesystem = fs_file.fs
    return _fetch_from_filesystem(fs_file.path, download_dir, filesystem)


def _fetch_from_json_filesystem(path_to_fetch, download_dir, fs):
    """Fetch a file from a path and a filesystem specification."""
    filesystem = fsspec.AbstractFileSystem.from_json(json.dumps(fs))
    return _fetch_from_filesystem(path_to_fetch, download_dir, filesystem)


def _fetch_from_filesystem(path_to_fetch, download_dir, fs):
    """Fetch a file from a path and a filesystem."""
    basename = unquote(os.path.basename(path_to_fetch))
    downloaded_file = download_dir / basename
    return _fetch_file(path_to_fetch, downloaded_file, fs)


def _fetch_file(remote_path, local_path, fs):
    """Fetch the remote path to the local path."""
    fs.get_file(remote_path, local_path)
    return local_path
