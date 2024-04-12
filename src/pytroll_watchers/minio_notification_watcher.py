"""Publish messages based on Minio bucket notifications."""

from upath import UPath

from pytroll_watchers.publisher import file_publisher_from_generator, fix_times, parse_metadata


def file_publisher(fs_config, publisher_config, message_config):
    """Publish files coming from bucket notifications.

    Args:
        fs_config: the configuration for the filesystem watching, will be passed as argument to `file_generator`.
        publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
        message_config: The information needed to complete the posttroll message generation. Will be amended
             with the file metadata, and passed directly to posttroll's Message constructor.
    """
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
            try:
                file_metadata = parse_metadata(file_pattern, new_file_name)
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
