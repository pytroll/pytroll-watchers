"""Module to provide file generator and publisher for the Copernicus dataspace contents.

It polls the catalogue using OData for new data (https://documentation.dataspace.copernicus.eu/APIs/OData.html) and
generates locations for the data on the S3 services (https://documentation.dataspace.copernicus.eu/APIs/S3.html).

Note:
    The OData and S3 services require two different set of credentials.

"""

import datetime
import logging
import netrc
import time
from contextlib import suppress
from functools import lru_cache

from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from upath import UPath

from pytroll_watchers.common import fromisoformat, run_every
from pytroll_watchers.publisher import file_publisher_from_generator

client_id = "cdse-public"
token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"  # noqa
logger = logging.getLogger(__name__)


def file_publisher(fs_config, publisher_config, message_config):
    """Publish files coming from local filesystem events.

    Args:
        fs_config: the configuration for the filesystem watching, will be passed as argument to `file_generator`.
        publisher_config: The configuration dictionary to pass to the posttroll publishing functions.
        message_config: The information needed to complete the posttroll message generation. Will be amended
             with the file metadata, and passed directly to posttroll's Message constructor.
    """
    logger.info(f"Starting watch on dataspace for '{fs_config['filter_string']}'")
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, publisher_config, message_config)


def file_generator(filter_string,
                   polling_interval,
                   dataspace_auth,
                   start_from=None,
                   storage_options=None):
    """Generate new objects by polling copernicus dataspace.

    Args:
        filter_string: the filter to use for narrowing the data to poll. For example, to poll level 1 olci data, it can
            be `contains(Name,'OL_1_EFR')`. For more information of the filter parameters, check:
            https://documentation.dataspace.copernicus.eu/APIs/OData.html
        polling_interval: the interval (timedelta object or kwargs to timedelta) at which the dataspace will be polled.
        dataspace_auth: the authentication information, as a dictionary. It can be a dictionary with `username` and
            `password` keys, or with `netrc_host` and optionaly `netrc_file` if the credentials are to be fetched with
            netrc.
        start_from: how far back in time to fetch the data the first time. This is helpful for the first iteration of
            the generator, so that data from the past can be fetched, to fill a possible gap. Default to 0, meaning
            nothing older than when the generator starts will be fetched. Same format accepted as polling_interval.
        storage_options: The options to pass the S3Path instance, usually include ways to get credentials to the
            copernicus object store, like `profile` from the .aws configuration files.

    Yields:
        Tuples of UPath (s3) and metadata.

    """
    with suppress(TypeError):
        polling_interval = datetime.timedelta(**polling_interval)
    with suppress(TypeError):
        start_from = datetime.timedelta(**start_from)
    if start_from is None:
        start_from = datetime.timedelta(0)

    last_pub_date = datetime.datetime.now(datetime.timezone.utc) - start_from

    for next_check in run_every(polling_interval):
        generator = generate_download_links_since(filter_string, dataspace_auth, last_pub_date, storage_options)
        for s3path, metadata in generator:
            last_pub_date = _update_last_publication_date(last_pub_date, metadata)
            yield s3path, metadata
        logger.info("Finished polling.")
        if next_check > datetime.datetime.now(datetime.timezone.utc):
            logger.info(f"Next iteration at {next_check}")


def _update_last_publication_date(last_publication_date, metadata):
    """Update the last publication data based on the metadata."""
    publication_date = fromisoformat(metadata.pop("PublicationDate"))
    if publication_date > last_publication_date:
        last_publication_date = publication_date
    return last_publication_date


def generate_download_links_since(filter_string, dataspace_auth, last_publication_date, storage_options):
    """Generate download links for data that was published since a given `last publication_date`.

    Example:
        To fetch download link since yesterday, using netrc-stored credentials, and an aws s3 profile:

        >>> from pytroll_watchers.dataspace_watcher import generate_download_links_since
        >>> filter_string = "contains(Name,'OL_1_EFR')"
        >>> dataspace_auth = dict(netrc_host="dataspace.copernicus.eu")
        >>> last_publication_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
        >>> storage_options = dict(profile="my_copernicus_s3_profile")
        >>> generator = generate_download_links_since(filter_string, dataspace_auth, last_publication_date,
        ...                                           storage_options)

    """
    pub_limit = f"PublicationDate gt {last_publication_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
    filter_string_with_pub_limit = f"{filter_string} and {pub_limit}"

    yield from generate_download_links(filter_string_with_pub_limit, dataspace_auth, storage_options)


def generate_download_links(filter_string, dataspace_auth, storage_options):
    """Generate download links for a given filter_string."""
    if storage_options is None:
        storage_options = {}
    oauth = _get_auth(**dataspace_auth)
    resp = oauth.get(filter_string)
    metadatas = resp.get("value", [])
    for metadata in metadatas:
        s3path = UPath("s3://" + metadata["S3Path"], **storage_options)
        mda = dict()
        attributes = _construct_attributes_dict(metadata)
        mda["platform_name"] = attributes["platformShortName"].capitalize() + attributes["platformSerialIdentifier"]
        mda["sensor"] = attributes["instrumentShortName"].lower()
        mda["PublicationDate"] = metadata["PublicationDate"]
        mda["boundary"] = metadata["GeoFootprint"]
        mda["product_type"] = attributes["productType"]
        mda["start_time"] = fromisoformat(attributes["beginningDateTime"])
        mda["end_time"] = fromisoformat(attributes["endingDateTime"])
        mda["orbit_number"] = int(attributes["orbitNumber"])

        for checksum in metadata["Checksum"]:
            if checksum["Algorithm"] == "MD5":
                mda["checksum"] = dict(algorithm=checksum["Algorithm"], hash=checksum["Value"])
                break
        mda["size"] = int(metadata["ContentLength"])

        yield s3path, mda


def _construct_attributes_dict(entry):
    """Construct a dict from then "results" item in entry."""
    results = entry["Attributes"]
    attributes = {result["Name"]: result["Value"] for result in results}
    return attributes


@lru_cache(maxsize=1)
def _get_auth(**dataspace_auth):
    oauth = CopernicusOAuth2Session(dataspace_auth)
    return oauth


class CopernicusOAuth2Session():
    """An oauth2 session for copernicus dataspace."""


    def __init__(self, dataspace_auth):
        """Set up the session."""
        dataspace_credentials = _get_credentials(dataspace_auth)
        self._oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
        def compliance_hook(response):
            response.raise_for_status()
            return response

        self._oauth.register_compliance_hook("access_token_response", compliance_hook)
        self._token_user, self._token_pass = dataspace_credentials

    def get(self, filter_string):
        """Run a get request."""
        self.fetch_token()
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter={filter_string}&$expand=Attributes"
        return self._oauth.get(url).json()

    def fetch_token(self):
        """Fetch the token."""
        if not self._oauth.token or self._oauth.token["expires_at"] <= time.time():
            self._oauth.fetch_token(token_url=token_url,
                                    username=self._token_user,
                                    password=self._token_pass)


def _get_credentials(ds_auth):
    """Get credentials from the ds_auth dictionary."""
    try:
        creds = ds_auth["username"], ds_auth["password"]
    except KeyError:
        username, _, password = netrc.netrc(ds_auth.get("netrc_file")).authenticators(ds_auth["netrc_host"])
        creds = (username, password)
    return creds
