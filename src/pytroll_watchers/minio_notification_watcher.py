from upath import UPath
from contextlib import closing


def file_publisher(s3_config, publisher_config, message_config):
    """Publish files coming from bucket notifications."""
    from posttroll.publisher import create_publisher_from_dict_config
    from posttroll.message import Message
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    with closing(publisher):
        for file_item in file_generator(**s3_config):
            amended_message_config = message_config.copy()
            amended_message_config["data"]["url"] = file_item.as_uri()
            amended_message_config["data"]["fs"] = file_item.fs.to_json()
            msg = Message(**amended_message_config)
            publisher.send(str(msg))


def file_generator(endpoint_url, bucket_name, profile=None):
    """Generate UPath instances for new files in the bucket."""

    for record in _record_generator(endpoint_url, bucket_name, profile):
        for item in record["Records"]:
            new_bucket_name = item["s3"]["bucket"]["name"]
            new_file_name = item["s3"]["object"]["key"]
            path = UPath(f"s3://{new_bucket_name}/{new_file_name}")
            yield path


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
