"""Watcher for DHuS instances.

For more information about DHuS, check out
https://sentineldatahub.github.io/DataHubSystem/about.html


An example configuration file to retrieve Sentinel 1 data from a DHuS instance:

.. code-block:: yaml

  backend: dhus
  fs_config:
    server: https://myhub.someplace.org/
    filter_params:
        - substringof('IW_GRDH',Name)
    polling_interval:
        seconds: 10
    start_from:
        hours: 6
  publisher_config:
    name: s1_watcher
  message_config:
    subject: /segment/s1/l1b/
    atype: file
    aliases:
        sensor:
          SAR: SAR-C


"""

import datetime as dt
import logging
from contextlib import suppress
from urllib.parse import urljoin

import requests
from geojson import Polygon
from upath import UPath

from pytroll_watchers.common import fromisoformat, run_every
from pytroll_watchers.publisher import file_publisher_from_generator

logger = logging.getLogger(__name__)


def file_publisher(fs_config, publisher_config, message_config):
    """Publish files coming from local filesystem events.

    Args:
        fs_config: the configuration for the filesystem watching, will be passed as argument to `file_generator`.
        publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
        message_config: The information needed to complete the posttroll message generation. Will be amended
             with the file metadata, and passed directly to posttroll's Message constructor.
    """
    logger.info(f"Starting watch on dhus for '{fs_config['filter_params']}'")
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, publisher_config, message_config)


def file_generator(server, filter_params, polling_interval, start_from=None):
    """Generate new objects by polling a DHuS instance.

    Args:
        server: the DHuS server to use.
        filter_params: the list of filter parameters to use for narrowing the data to poll. For example, to poll IW sar
            data, it can be `substringof('IW_GRDH',Name)`. For more information of the filter parameters, check:
            https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/ODataAPI#filter
        polling_interval: the interval (timedelta object or kwargs to timedelta) at which the DHUS will be polled.
        start_from: how far back in time to fetch the data the first time. This is helpful for the first iteration of
            the generator, so that data from the past can be fetched, to fill a possible gap. Default to 0, meaning
            nothing older than when the generator starts will be fetched. Same format accepted as polling_interval.

    Yields:
        Tuples of UPath (http) and metadata.


    Note:
        As this watcher uses requests, the authentication information should be stored in a .netrc file.
    """
    with suppress(TypeError):
        polling_interval = dt.timedelta(**polling_interval)
    with suppress(TypeError):
        start_from = dt.timedelta(**start_from)
    if start_from is None:
        start_from = dt.timedelta(0)

    last_pub_date = dt.datetime.now(dt.timezone.utc) - start_from
    for next_check in run_every(polling_interval):
        generator = generate_download_links_since(server, filter_params, last_pub_date)
        for path, metadata in generator:
            last_pub_date = _update_last_publication_date(last_pub_date, metadata)
            yield path, metadata
        logger.info("Finished polling.")
        if next_check > dt.datetime.now(dt.timezone.utc):
            logger.info(f"Next iteration at {next_check}")


def _update_last_publication_date(last_publication_date, metadata):
    """Update the last publication data based on the metadata."""
    publication_date = metadata.pop("ingestion_date")
    if publication_date > last_publication_date:
        last_publication_date = publication_date
    return last_publication_date


def generate_download_links_since(server, filter_params, last_publication_date):
    """Generate download links for the data published since `last_publication_date`."""
    last_publication_date = last_publication_date.astimezone(dt.timezone.utc)
    # remove timezone info as dhus considers all times utc
    pub_string = last_publication_date.isoformat(timespec="milliseconds")[:-6]
    filter_params = filter_params + [f"IngestionDate gt datetime'{pub_string}'"]
    yield from generate_download_links(server, filter_params)


def generate_download_links(server, filter_params):
    """Generate download links.

    The filter params we can use are defined here: https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/ODataAPI#filter
    """
    filter_string = " and ".join(filter_params)

    url = urljoin(server, f"/odata/v1/Products?$format=json&$filter={filter_string}&$expand=Attributes")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    entries = response.json()

    for entry in entries["d"]["results"]:
        mda = dict()
        path = UPath(entry["__metadata"]["media_src"])
        mda["boundary"] = _extract_boundary_as_geojson(entry)
        attributes = _construct_attributes_dict(entry)
        mda["platform_name"] = attributes["Satellite name"].capitalize() + attributes["Satellite number"]
        mda["sensor"] = attributes["Instrument"]
        mda["ingestion_date"] = fromisoformat(attributes["Ingestion Date"])
        mda["product_type"] = attributes["Product type"]
        mda["start_time"] = fromisoformat(attributes["Sensing start"])
        mda["end_time"] = fromisoformat(attributes["Sensing stop"])
        mda["orbit_number"] = int(attributes["Orbit number (start)"])

        mda["checksum"] = dict(algorithm=entry["Checksum"]["Algorithm"], hash=entry["Checksum"]["Value"])
        mda["size"] = int(entry["ContentLength"])
        mda["uid"] = attributes["Filename"]
        yield path, mda

def _construct_attributes_dict(entry):
    """Construct a dict from then "results" item in entry."""
    results = entry["Attributes"]["results"]
    results_dict = {result["Name"]: result["Value"] for result in results}
    return results_dict


def _extract_boundary_as_geojson(entry):
    """Extract the boundary from the entry metadata."""
    gml, nsmap = read_gml(entry["ContentGeometry"])
    boundary_text = gml.find("gml:outerBoundaryIs/gml:LinearRing/gml:coordinates", namespaces=nsmap).text
    boundary_list = (coords.split(",") for coords in boundary_text.strip().split(" "))
    boundary = Polygon([[(float(lon), float(lat)) for (lat, lon) in boundary_list]])
    return boundary

def read_gml(gml_string):
    """Read the gml string."""
    from xml.etree import ElementTree

    from defusedxml.ElementTree import DefusedXMLParser
    parser = ElementTree.XMLPullParser(["start-ns", "end"], _parser=DefusedXMLParser())
    parser.feed(gml_string)

    nsmap = dict()

    for event, elem in parser.read_events():
        if event == "start-ns":
            nsmap[elem[0]] = elem[1]
    return elem, nsmap
