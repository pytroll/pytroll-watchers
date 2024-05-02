"""Tests for the dhus watcher."""


import datetime as dt

import responses
import responses._recorder
from freezegun import freeze_time
from posttroll.message import Message
from posttroll.testing import patched_publisher
from pytroll_watchers.dhus_watcher import (
    file_generator,
    file_publisher,
    generate_download_links,
    generate_download_links_since,
)

server = "https://colhub.met.no"


@responses.activate
def test_generate_download_links():
    """Test the generation of download links."""
    responses._add_from_file(file_path="tests/dhus_responses.yaml")

    filter_params = ["substringof('IW_GRDH',Name)",
                     "IngestionDate gt datetime'2024-04-29T07:33:00.000'"]

    links = list(generate_download_links(server, filter_params))
    assert len(links) == 5
    path, mda = links[0]
    assert path.as_uri() == "https://colhub.met.no/odata/v1/Products('e49f8de5-1647-4a7b-ba69-268f9aa77f42')/$value"
    assert mda["boundary"]["coordinates"][0][0] == [-4.545108, 57.399067]

    assert mda["platform_name"] == "Sentinel-1A"
    assert mda["sensor"] == "SAR"
    assert "ingestion_date" in mda
    assert mda["product_type"] == "IW_GRDH_1S"
    assert mda["start_time"] == dt.datetime(2024, 4, 29, 6, 46, 23, 104000, tzinfo=dt.timezone.utc)
    assert mda["end_time"] == dt.datetime(2024, 4, 29, 6, 46, 48, 102000, tzinfo=dt.timezone.utc)
    assert mda["orbit_number"] == 53645
    assert "checksum" in mda
    assert "size" in mda


@responses.activate
def test_generate_download_links_since():
    """Test limiting the download links to a given point in time."""
    responses._add_from_file(file_path="tests/dhus_responses.yaml")

    filter_params = ["substringof('IW_GRDH',Name)"]
    since = dt.datetime(2024, 4, 29, 7, 33, tzinfo=dt.timezone.utc)
    links = list(generate_download_links_since(server, filter_params, since))
    assert len(links) == 5
    path, mda = links[1]
    assert path.as_uri() == "https://colhub.met.no/odata/v1/Products('162f165a-23c3-4212-869e-4f38b9dade63')/$value"
    assert mda["start_time"] == dt.datetime(2024, 4, 29, 6, 52, 38, 105000, tzinfo=dt.timezone.utc)


@freeze_time(dt.datetime(2024, 4, 29, 8, 33, tzinfo=dt.timezone.utc))
@responses.activate
def test_file_generator():
    """Test the file generator."""
    responses._add_from_file(file_path="tests/dhus_responses.yaml")

    filter_params = ["substringof('IW_GRDH',Name)"]
    since = dt.timedelta(hours=1)
    polling_interval = dict(minutes=0)
    links = list(file_generator(server, filter_params, polling_interval, since))
    assert len(links) == 5
    path, mda = links[3]
    assert path.as_uri() == "https://colhub.met.no/odata/v1/Products('a6b214dd-fe39-4f50-aa94-f84d8510cedc')/$value"
    assert mda["start_time"] == dt.datetime(2024, 4, 29, 6, 54, 18, 105000, tzinfo=dt.timezone.utc)
    assert "ingestion_date" not in mda
    assert mda["uid"] == "S1A_IW_GRDH_1SDV_20240429T065418_20240429T065443_053645_0683C0_8D1D.SAFE"


@freeze_time(dt.datetime(2024, 4, 29, 8, 33, tzinfo=dt.timezone.utc))
@responses.activate
def test_publish_paths():
    """Test publishing paths."""
    responses._add_from_file(file_path="tests/dhus_responses.yaml")

    filter_params = ["substringof('IW_GRDH',Name)"]

    publisher_settings = dict(nameservers=False, port=1979)
    message_settings = dict(subject="/segment/olci/l1b/", atype="file", aliases=dict(sensor={"SAR": "SAR-C"}))

    with patched_publisher() as messages:
        fs_config = dict(server=server,
                         filter_params=filter_params,
                         polling_interval=dict(minutes=0),
                         start_from=dict(hours=1))


        file_publisher(fs_config=fs_config,
                        publisher_config=publisher_settings,
                        message_config=message_settings)
    message = Message(rawstr=messages[3])

    assert message.data["uid"] == "S1A_IW_GRDH_1SDV_20240429T065418_20240429T065443_053645_0683C0_8D1D.SAFE"
    assert message.data["sensor"] == "SAR-C"
