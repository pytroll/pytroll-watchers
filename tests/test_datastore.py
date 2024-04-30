"""Test watching the EUMETSAT Datastore."""

import datetime
from contextlib import contextmanager

import pytest
import responses
import yaml
from freezegun import freeze_time
from posttroll.message import Message
from posttroll.testing import patched_publisher
from pytroll_watchers.datastore_watcher import file_generator, file_publisher, generate_download_links_since


@pytest.fixture()
def search_params():
    """Generate the search parameters for the tests."""
    polygon = ("POLYGON((9.08 60.00,16.25 59.77,17.50 63.06,21.83 66.02,26.41 65.75,22.22 60.78,28.90 60.82,30.54 60.01"
               ",20.20 53.57,9.06 53.50,9.08 60.00))")
    collection = "EO:EUM:DAT:0407"
    return dict(collection=collection, geo=polygon)


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_datastore_get_download_links_since(search_params):
    """Test downloading links from the datastore."""
    ds_auth = dict(username="user", password="pass")  # noqa

    now = datetime.datetime.now(datetime.timezone.utc)
    yesterday = now - datetime.timedelta(hours=24)

    str_pub_start = yesterday.isoformat(timespec="milliseconds")


    response_file = "tests/datastore_responses.yaml"


    with loaded_responses(str_pub_start, response_file):

        search_params = dict(collection=search_params["collection"], geo=search_params["geo"])

        features = generate_download_links_since(search_params, ds_auth, yesterday)
        features = list(features)

    expected_token = "eceba4e1-95e6-3526-8c42-c3c9dc14ff5c"  # noqa

    assert len(features) == 5
    path, mda = features[0]
    assert str(path) == "https://api.eumetsat.int/data/download/1.0.0/collections/EO%3AEUM%3ADAT%3A0407/products/S3B_OL_2_WFR____20240416T104217_20240416T104517_20240417T182315_0180_092_051_1980_MAR_O_NT_003.SEN3"
    assert expected_token in path.storage_options["client_kwargs"]["headers"]["Authorization"]
    assert mda["boundary"]["coordinates"][0][0] == [-14.3786, 52.4516]
    assert mda["platform_name"] == "Sentinel-3B"
    assert mda["sensor"] == "olci"
    assert mda["start_time"] == datetime.datetime(2024, 4, 16, 10, 42, 16, 954262, tzinfo=datetime.timezone.utc)
    assert mda["end_time"] == datetime.datetime(2024, 4, 16, 10, 45, 16, 954262, tzinfo=datetime.timezone.utc)
    assert mda["orbit_number"] == 31123
    assert mda["product_type"] == "EO:EUM:DAT:0407"
    assert mda["checksum"] == dict(algorithm="md5", hash="9057eb08f2a4e9f4c5a8d2eeaacedaef")


@contextmanager
def loaded_responses(since, response_file):
    """Load prerecorded responses for querying the datastore."""
    with responses.RequestsMock() as rsps:
        with open(response_file) as fd:
            contents = yaml.safe_load(fd.read())
            for response in contents["responses"]:
                resp = response["response"]
                resp["url"] = resp["url"].replace("{yesterday}", since.replace("+", "%2B"))
                rsps.add(**response["response"])

        yield


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_datastore_file_generator(tmp_path, search_params):
    """Test the file generator."""
    netrc_host = "myitem"
    netrc_file = tmp_path / "netrc"

    with open(netrc_file, "w") as fd:
        fd.write(f"machine {netrc_host} login user@pytroll.org password mypassword")

    ds_auth = dict(netrc_host=netrc_host, netrc_file=netrc_file)


    now =datetime.datetime.now(datetime.timezone.utc)
    yesterday = now - datetime.timedelta(hours=24)
    start_from = dict(hours=24)


    polling_interval = datetime.timedelta(0)


    str_yesterday = yesterday.isoformat(timespec="milliseconds")
    response_file = "tests/datastore_responses.yaml"

    with loaded_responses(str_yesterday, response_file):
        features = file_generator(search_params, polling_interval, ds_auth, start_from)
        features = list(features)

    expected_token = "eceba4e1-95e6-3526-8c42-c3c9dc14ff5c"  # noqa

    assert len(features) == 5
    path, _ = features[0]
    assert str(path) == "https://api.eumetsat.int/data/download/1.0.0/collections/EO%3AEUM%3ADAT%3A0407/products/S3B_OL_2_WFR____20240416T104217_20240416T104517_20240417T182315_0180_092_051_1980_MAR_O_NT_003.SEN3"
    assert expected_token in path.storage_options["client_kwargs"]["headers"]["Authorization"]


@freeze_time(datetime.datetime.now(datetime.timezone.utc))
def test_publish_paths(caplog, search_params):
    """Test publishing paths."""
    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l2/", atype="file", data=dict(sensor="olci"))


    now =datetime.datetime.now(datetime.timezone.utc)
    yesterday = now - datetime.timedelta(hours=24)
    str_yesterday = yesterday.isoformat(timespec="milliseconds")
    response_file = "tests/datastore_responses.yaml"

    caplog.set_level("INFO")
    with patched_publisher() as messages:
        with loaded_responses(str_yesterday, response_file):
            ds_config = dict(search_params=search_params,
                             polling_interval=dict(minutes=0),
                             start_from=dict(hours=24),
                             ds_auth=dict(username="user5", password="pass5"))  # noqa


            file_publisher(fs_config=ds_config,
                           publisher_config=publisher_settings,
                           message_config=message_settings)
    assert "uri" not in message_settings["data"]
    assert len(messages) == 5
    message = Message(rawstr=messages[0])
    uri = "https://api.eumetsat.int/data/download/1.0.0/collections/EO%3AEUM%3ADAT%3A0407/products/S3B_OL_2_WFR____20240416T104217_20240416T104517_20240417T182315_0180_092_051_1980_MAR_O_NT_003.SEN3"
    assert message.data["uri"] == uri
    assert message.data["sensor"] == "olci"
    assert message.data["filesystem"] == {
        "cls": "fsspec.implementations.http.HTTPFileSystem",
        "protocol": "https",
        "args": [],
        "encoded": True,
        "client_kwargs": {"headers": {"Authorization": "Bearer eceba4e1-95e6-3526-8c42-c3c9dc14ff5c"}},
    }
    assert message.data["path"] == uri
    assert f"Starting watch on datastore for '{search_params}'" in caplog.text
