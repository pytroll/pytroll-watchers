"""Test s3 file poller."""

import os
from collections.abc import Generator
from datetime import datetime, timedelta, timezone

import pytest
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from s3fs import S3FileSystem
from upath import UPath

from pytroll_watchers.s3_poller import _poll_files

filenames = ["sat_20250828T1000.dat", "sat_20250828T1010.dat"]
file_pattern = "{platform_name}_{start_time:%Y%m%dT%H%M}.dat"


@pytest.fixture(scope="session", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa
    os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa
    os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# Note: pick an appropriate fixture "scope" for your use case
@pytest.fixture(scope="session")
def endpoint() -> Generator[str]:
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def bucket(endpoint: str) -> Generator[str]:
    """Create a bucket."""
    from botocore.session import Session

    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint)
    bucketname = "test"
    client.create_bucket(Bucket=bucketname, ACL="public-read")
    yield bucketname
    client.delete_bucket(Bucket=bucketname)


def create_file(s3: S3FileSystem, filename: str):
    """Create a dummy file."""
    data = "very important data"
    with s3.open(filename, "w") as f:
        f.write(data)


@pytest.fixture
def some_files(endpoint: str, bucket: str):
    """Create a couple of files."""
    s3 = S3FileSystem(endpoint_url=endpoint)
    for filename in filenames:
        create_file(s3, bucket + "/" + filename)
    yield

    for filename in filenames:
        s3.rm_file(bucket + "/" + filename)


def test_file_poll(endpoint: str, bucket: str, some_files):
    """Test polling."""
    assert list(_poll_files(bucket, endpoint_url=endpoint).keys()) == [bucket + "/" + f for f in filenames]
    assert list(_poll_files(bucket, "sat_{time}",
                            endpoint_url=endpoint).keys()) == [bucket + "/" + f for f in filenames]
    assert list(_poll_files(bucket, filenames[0], endpoint_url=endpoint).keys()) == [bucket + "/" + filenames[0]]


def test_poll_files_since(endpoint: str, bucket: str, some_files):
    """Test polling since."""
    before = datetime.now(timezone.utc) - timedelta(seconds=2)
    assert list(_poll_files(bucket, start_date=before, endpoint_url=endpoint)) == [bucket + "/" + f for f in filenames]
    assert list(_poll_files(bucket, start_date=datetime.now(timezone.utc), endpoint_url=endpoint)) == []


def test_file_generator(endpoint: str, bucket: str):
    """Test the file generator."""
    from pytroll_watchers.s3_poller import file_generator
    s3 = S3FileSystem(endpoint_url=endpoint)
    gen = file_generator(bucket, start_from=timedelta(seconds=2),
                         polling_interval=timedelta(seconds=0.01),
                         storage_options=dict(endpoint_url=endpoint))
    filenames = [f"f{num}.file" for num in range(10)]
    try:
        for filename in filenames:
            create_file(s3, bucket + "/" + filename)
            f, _ = next(gen)
            assert f == UPath(bucket + "/" + filename, protocol="s3", endpoint_url=endpoint)
    finally:
        for filename in filenames:
            s3.rm(bucket + "/" + filename)


def test_file_generator_with_dicts(endpoint: str, bucket: str):
    """Test the file generator."""
    from pytroll_watchers.s3_poller import file_generator
    s3 = S3FileSystem(endpoint_url=endpoint)
    gen = file_generator(bucket, start_from=dict(seconds=2),
                         polling_interval=dict(seconds=0.01),
                         storage_options=dict(endpoint_url=endpoint))
    try:
        for filename in ["f1", "f2"]:
            create_file(s3, bucket + "/" + filename)
            f, _ = next(gen)
            assert f == UPath(bucket + "/" + filename, protocol="s3", endpoint_url=endpoint)
    finally:
        for filename in ["f1", "f2"]:
            s3.rm(bucket + "/" + filename)


def test_generate_download_links(endpoint: str, bucket: str, some_files):
    """Test link generation."""
    from pytroll_watchers.s3_poller import generate_download_links
    for (path, mda), f in zip(generate_download_links(bucket, file_pattern=file_pattern,
                                                      storage_options=dict(endpoint_url=endpoint)),
                              filenames,
                              strict=False):
        assert path.path == bucket + "/" + f
        assert path.protocol == "s3"
        assert path.storage_options == dict(endpoint_url=endpoint)
        assert mda["platform_name"] == "sat"
        assert mda["start_time"] in (datetime(2025, 8, 28, 10, 0), datetime(2025, 8, 28, 10, 10))


def test_generate_download_links_since(endpoint: str, bucket: str, some_files):
    """Test link generation from date."""
    from pytroll_watchers.s3_poller import generate_download_links_since
    assert list(generate_download_links_since(bucket, start_from=datetime.now(timezone.utc),
                                              storage_options=dict(endpoint_url=endpoint))) == []


def test_file_publisher(endpoint:str, bucket: str, some_files):
    """Test the file publisher."""
    from posttroll.testing import patched_publisher

    from pytroll_watchers.s3_poller import file_publisher

    config = dict(fs_config=dict(bucket_name=bucket,
                                 storage_options=dict(endpoint_url=endpoint),
                                 start_from=dict(seconds=2),
                                 polling_interval=timedelta(0)),
                  publisher_config=dict(name="s3"),
                  message_config=dict(subject="hej",
                                      atype="file"))
    with patched_publisher() as messages:
        file_publisher(config)
        assert filenames[0] in messages[0]
        assert filenames[1] in messages[1]
