"""Tests for the copernicus datapace watcher."""
import datetime

from freezegun import freeze_time
from posttroll.message import Message
from posttroll.testing import patched_publisher
from pytroll_watchers.dataspace_watcher import file_generator, file_publisher
from pytroll_watchers.testing import load_oauth_responses


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_dataspace_watcher():
    """Test the dataspace watcher."""
    check_interval = datetime.timedelta(0)  # run once and avoid the infinite loop
    filter_string = "contains(Name,'OL_1_EFR')"
    storage_options = dict(profile="mycopernicus")
    username = "user1"
    password = "pass1"  # noqa
    auth = dict(username=username, password=password)
    with load_oauth_responses("token", "filtered_yesterday"):
        files = list(file_generator(filter_string, check_interval, auth, datetime.timedelta(hours=24),
                                    storage_options))
        assert len(files) == 3
        s3path, metadata = files[0]
        assert s3path.storage_options == storage_options
        fname = "S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3"
        assert s3path.as_uri().endswith(fname)
        assert metadata["platform_name"] == "Sentinel-3B"
        assert metadata["sensor"] == "olci"
        assert metadata["start_time"] == datetime.datetime(2024, 4, 15, 7, 40, 29, 480000, tzinfo=datetime.timezone.utc)
        assert metadata["end_time"] == datetime.datetime(2024, 4, 15, 7, 43, 29, 480000, tzinfo=datetime.timezone.utc)
        assert metadata["boundary"]["coordinates"][0][0] == [67.8172, 69.3862]
        assert metadata["orbit_number"] == 31107


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_dataspace_watcher_without_storage_options():
    """Check that not providing storage options work."""
    check_interval = datetime.timedelta(0)
    filter_string = "contains(Name,'OL_1_EFR')"
    username = "user2"
    password = "pass2"  # noqa
    auth = dict(username=username, password=password)
    with load_oauth_responses("token", "filtered_yesterday"):
        files = list(file_generator(filter_string, check_interval, auth, start_from=datetime.timedelta(hours=24)))
        s3path, _ = files[0]
        assert s3path.storage_options == {}


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_dataspace_watcher_default_start_time():
    """Test that the dataspace watcher uses now as the default start time."""
    check_interval = datetime.timedelta(0)  # run once and avoid the infinite loop
    filter_string = "contains(Name,'OL_1_EFR')"
    username = "user3"
    password = "pass3"  # noqa
    auth = dict(username=username, password=password)
    with load_oauth_responses("token", "filtered_today"):
        files = list(file_generator(filter_string, check_interval, auth))
        assert len(files) == 0


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_dataspace_watcher_with_token_credentials_from_netrc(tmp_path):
    """Check that authentication can use netrc."""
    check_interval = datetime.timedelta(0)
    filter_string = "contains(Name,'OL_1_EFR')"
    netrc_host = "myitem"
    netrc_file = tmp_path / "netrc"

    with open(netrc_file, "w") as fd:
        fd.write(f"machine {netrc_host} login user@pytroll.org password mypassword")

    auth = dict(netrc_host=netrc_host, netrc_file=netrc_file)
    with load_oauth_responses("token", "filtered_yesterday"):
        files = list(file_generator(filter_string, check_interval, auth,
                                    start_from=datetime.timedelta(hours=24)))
        s3path, _ = files[0]
        assert s3path.storage_options == {}


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_dataspace_watcher_logs(caplog):
    """Check that logging works."""
    caplog.set_level("INFO")
    check_interval = datetime.timedelta(0)
    filter_string = "contains(Name,'OL_1_EFR')"
    username = "user4"
    password = "pass4"  # noqa
    auth = dict(username=username, password=password)
    with load_oauth_responses("token", "filtered_yesterday"):
        list(file_generator(filter_string, check_interval, auth, start_from=datetime.timedelta(hours=24)))
    assert "Finished polling." in caplog.text


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_publish_paths(caplog):
    """Test publishing paths."""
    filter_string = "contains(Name,'OL_1_EFR')"

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l1b/", atype="file", data=dict(sensor="olci"))

    caplog.set_level("INFO")
    with patched_publisher() as messages:
        with load_oauth_responses("token", "filtered_yesterday"):
            ds_config = dict(filter_string=filter_string,
                            polling_interval=dict(minutes=0),
                            start_from=dict(hours=24),
                            dataspace_auth=dict(username="user5", password="pass5"),  # noqa
                            storage_options=dict(profile="someprofile"))


            file_publisher(fs_config=ds_config,
                           publisher_config=publisher_settings,
                           message_config=message_settings)
    assert "uri" not in message_settings["data"]
    assert len(messages) == 3
    message = Message(rawstr=messages[0])
    assert message.data["uri"] == "s3:///eodata/Sentinel-3/OLCI/OL_1_EFR___/2024/04/15/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3"
    assert message.data["uid"] == "S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3"  # noqa
    assert message.data["sensor"] == "olci"
    assert message.data["filesystem"] == {"cls": "s3fs.core.S3FileSystem", "protocol": "s3", "args": [],
                                  "profile": "someprofile"}
    assert message.data["path"] == "/eodata/Sentinel-3/OLCI/OL_1_EFR___/2024/04/15/S3B_OL_1_EFR____20240415T074029_20240415T074329_20240415T094236_0179_092_035_1620_PS2_O_NR_003.SEN3"  # noqa
    assert f"Starting watch on dataspace for '{filter_string}'" in caplog.text
