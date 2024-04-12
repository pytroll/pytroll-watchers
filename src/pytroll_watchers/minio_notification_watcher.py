"""Publish messages based on Minio bucket notifications."""

import datetime

from trollsift import parse
from upath import UPath

from pytroll_watchers.publisher import file_publisher_from_generator


def file_publisher(fs_config, publisher_config, message_config):
    """Publish files coming from bucket notifications."""
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, publisher_config, message_config)


def file_generator(endpoint_url, bucket_name, file_pattern=None, storage_options=None):
    """Generate new files appearing in the watched directory.

    Args:
        endpoint_url: The endpoint_url to use.
        bucket_name: The bucket to watch for changes.
        file_pattern: The trollsift pattern to use for matching and extracting metadata from the filename.
            This must not include the directory.
        storage_options: The storage options for the service, for example for specifying a profile to the aws config.

    Returns:
        A tuple of UPath and file metadata.

    Examples:
        To iterate over new files in `s3:///tmp/`:

        >>> for filename in file_generator("some_endpoint_url", "tmp",
        ...                                file_pattern="{start_time:%Y%m%d_%H%M}_{product}.tif")
        ...    print(filename)
        UPath("s3:///tmp/20200428_1000_foo.tif")

    """
    file_metadata = {}

    if storage_options is None:
        storage_options = {}
    for record in _record_generator(endpoint_url, bucket_name, storage_options):
        for item in record["Records"]:
            new_bucket_name = item["s3"]["bucket"]["name"]
            new_file_name = item["s3"]["object"]["key"]
            if file_pattern is not None:
                try:
                    file_metadata = parse(file_pattern, new_file_name)
                    fix_times(file_metadata)
                except ValueError:
                    continue

            path = UPath(f"s3://{new_bucket_name}/{new_file_name}", **storage_options)
            yield path, file_metadata


def _record_generator(endpoint_url, bucket_name, profile=None):
    """Generate records for new files in the bucket."""
    from minio import Minio
    from minio.credentials.providers import AWSConfigProvider

    if profile is not None:
        credentials = AWSConfigProvider(profile=profile)

    client = Minio(endpoint_url,
        credentials=credentials
    )

    with client.listen_bucket_notification(
        bucket_name,
        # prefix="my-prefix/",
        events=["s3:ObjectCreated:*"],
    ) as events:
        for event in events:
            yield event


def fix_times(info):
    """Fix times so that date and time components are combined."""
    if "start_date" in info:
        info["start_time"] = datetime.datetime.combine(info["start_date"].date(),
                                                       info["start_time"].time())
        if "end_date" not in info:
            info["end_date"] = info["start_date"]
        del info["start_date"]
    if "end_date" in info:
        info["end_time"] = datetime.datetime.combine(info["end_date"].date(),
                                                     info["end_time"].time())
        del info["end_date"]
    if "end_time" in info:
        while info["start_time"] > info["end_time"]:
            info["end_time"] += datetime.timedelta(days=1)
