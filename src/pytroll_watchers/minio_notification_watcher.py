"""Publish messages based on bucket notifications."""

import datetime
from contextlib import closing
from copy import deepcopy

from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from trollsift import parse
from upath import UPath


def file_publisher(s3_config, publisher_config, message_config):
    """Publish files coming from bucket notifications."""
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    with closing(publisher):
        for file_item, file_metadata in file_generator(**s3_config):
            amended_message_config = deepcopy(message_config)
            amended_message_config["data"]["uri"] = file_item.as_uri()
            amended_message_config["data"]["fs"] = file_item.fs.to_json()
            amended_message_config["data"].update(file_metadata)
            msg = Message(**amended_message_config)
            publisher.send(str(msg))


def file_generator(endpoint_url, bucket_name, file_pattern=None, profile=None):
    """Generate UPath instances for new files in the bucket."""
    file_metadata = {}
    for record in _record_generator(endpoint_url, bucket_name, profile):
        for item in record["Records"]:
            new_bucket_name = item["s3"]["bucket"]["name"]
            new_file_name = item["s3"]["object"]["key"]
            if file_pattern is not None:
                try:
                    file_metadata = parse(file_pattern, new_file_name)
                    fix_times(file_metadata)
                except ValueError:
                    continue
            path = UPath(f"s3://{new_bucket_name}/{new_file_name}")
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
