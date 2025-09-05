"""Module to provide file generator and publisher for the EUMETSAT datastore contents.

It polls the catalogue using Opensearch for new data and generates locations for the data on https.

Note:
    The links produced can only be downloaded with a valid token. A token comes with the links, but
    has only a limited validity time (maybe 5 minutes).

An example for getting links to MSG data::

    from pytroll_watchers.datastore_watcher import generate_download_links_since

    ds_auth = dict(netrc_host="api.eumetsat.int")

    collection = "EO:EUM:DAT:MSG:HRSEVIRI"

    search_params = dict(collection=collection)
    now = datetime.datetime.now(datetime.timezone.utc)
    yesterday = now - datetime.timedelta(hours=6)

    links = list(generate_download_links_since(search_params, ds_auth, yesterday))

Another example, here a configuration file to pass to the CLI::

    backend: datastore
    fs_config:
      search_params:
        collection: "EO:EUM:DAT:0905"
      polling_interval:
        minutes: 5
      start_from:
        hours: 6
      ds_auth:
        netrc_host: api.eumetsat.int

    publisher_config:
      name: <my watcher publisher name>
      nameservers: false
      port: <my port number>
    message_config:
      subject: /my/datastore/watcher/topic
      atype: file
      data:
        sensor: aws
        platform_name: AWS1
        variant: GDS
"""

import datetime
import logging
import netrc
import time
from contextlib import suppress

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from upath import UPath

from pytroll_watchers.common import fromisoformat, run_every
from pytroll_watchers.publisher import file_publisher_from_generator
from pytroll_watchers.version import version

logger = logging.getLogger(__name__)

token_url = "https://api.eumetsat.int/token"  # noqa
data_url = "https://api.eumetsat.int/data"



def file_publisher(config):
    """Publish files coming from local filesystem events.

    Args:
        config: the configuration dictionary, containing in particular an fs_config section, which is the configuration
        for the filesystem watching, will be passed as argument to `file_generator`. The other sections are passed
        further to ``file_publisher_from_generator``.
    """
    fs_config = config["fs_config"]
    logger.info(f"Starting watch on datastore for '{fs_config['search_params']}'")
    generator = file_generator(**fs_config)
    return file_publisher_from_generator(generator, config)


def file_generator(search_params, polling_interval, ds_auth, start_from=None):
    """Search params must contain at least collection.

    Args:
        search_params: the dictionary of search parameters to request. Based on the opensearch API:
          https://user.eumetsat.int/api-definitions/data-store-opensearch-api
        polling_interval: how often to poll for new data. Can be provided as a timedelta or a dictionary of arguments
          for timedelta.
        ds_auth: either a dictionary with `netrc_host` (and optionally `netrc_file`), or a dictionary with `username`
          and `password`.
        start_from: a timedelta or dictionary of arguments to timedelta to specify how far in time to start fetching
          data. `None` by default, which means the data will be no older that now.
    """
    with suppress(TypeError):
        polling_interval = datetime.timedelta(**polling_interval)
    with suppress(TypeError):
        start_from = datetime.timedelta(**start_from)
    if start_from is None:
        start_from = datetime.timedelta(0)

    last_pub_date = datetime.datetime.now(datetime.timezone.utc) - start_from
    for next_check in run_every(polling_interval):
        new_pub_date =  datetime.datetime.now(datetime.timezone.utc)
        yield from generate_download_links_since(search_params, ds_auth, last_pub_date)
        logger.info("Finished polling.")
        if next_check > datetime.datetime.now(datetime.timezone.utc):
            logger.info(f"Next iteration at {next_check}")
            last_pub_date = new_pub_date

def generate_download_links_since(search_params, ds_auth, start_from):
    """Generate download links for data that was published since `start_from`."""
    str_pub_start = start_from.isoformat(timespec="milliseconds")
    search_params = search_params.copy()
    search_params["publication"] = f"[{str_pub_start}"
    yield from generate_download_links(search_params, ds_auth)


def generate_download_links(search_params, ds_auth):
    """Generate download links provide search parameter and authentication."""
    session = DatastoreOAuth2Session(ds_auth)
    collection = search_params.pop("collection")
    request_params = {
            "format": "json",
            "pi": str(collection),
            "si": 0,
            "c": 100,  # items per page
        }

    if search_params:
        request_params.update(search_params)

    jres = session.get(request_params)
    headers={"Authorization": f"Bearer {session.token['access_token']}"}
    client_args = dict(headers=headers)
    features = _get_features(jres)
    for feature in features:
        links = feature["properties"]["links"]["data"]
        if len(links) != 1:
            raise ValueError("Don't know how to generate multiple files at the time.")
        path = UPath(links[0]["href"], encoded=True, client_kwargs=client_args)
        # In the future, it might be interesting to generate items from the sip-entries, as
        # they contain individual files for zip archives.
        mda = dict()
        mda["boundary"] = feature["geometry"]
        acq_info = feature["properties"]["acquisitionInformation"][0]
        mda["platform_name"] = acq_info["platform"]["platformShortName"]
        mda["sensor"] = acq_info["instrument"]["instrumentShortName"].lower()
        with suppress(KeyError):
            mda["orbit_number"] = acq_info["acquisitionParameters"]["orbitNumber"]
        start_string, end_string = feature["properties"]["date"].split("/")
        mda["start_time"] = fromisoformat(start_string)
        mda["end_time"] = fromisoformat(end_string)
        mda["product_type"] = str(collection)
        mda["checksum"] = dict(algorithm="md5", hash=feature["properties"]["extraInformation"]["md5"])
        yield path, mda


def _get_features(jres):
    """Get the features from the session request."""
    try:
        return jres["features"]
    except KeyError as exc:
        if "type" in jres and jres["type"] == "ExceptionReport":
            raise IOError(f"Failed getting features! {str(jres['exceptions'])}") from None
        else:
            raise AttributeError("Failed getting features from jason result!") from exc


class DatastoreOAuth2Session():
    """An oauth2 session for eumetsat datastore."""

    def __init__(self, datastore_auth):
        """Set up the session."""
        client_id, client_secret = _get_credentials(datastore_auth)
        self._oauth = OAuth2Session(client=BackendApplicationClient(client_id=client_id))
        def compliance_hook(response):
            response.raise_for_status()
            return response

        self._oauth.register_compliance_hook("access_token_response", compliance_hook)
        self._token_secret = client_secret

    def get(self, params):
        """Run a get request."""
        self.fetch_token()
        search_url = f"{data_url}/search-products/1.0.0/os"
        headers = {"referer": "https://github.com/pytroll/pytroll-watchers",
                   "User-Agent": f"pytroll-watchers / {version}"}

        return self._oauth.get(search_url, params=params, headers=headers).json()

    @property
    def token(self):
        """Return the current token."""
        return self.fetch_token()

    def fetch_token(self):
        """Fetch the token."""
        if not self._oauth.token or self._oauth.token["expires_at"] <= time.time():
            self._oauth.fetch_token(token_url=token_url,
                                    client_secret=self._token_secret,
                                    include_client_id=True)
        return self._oauth.token


def _get_credentials(ds_auth):
    """Get credentials from the ds_auth dictionary."""
    try:
        creds = ds_auth["username"], ds_auth["password"]
    except KeyError:
        username, _, password = netrc.netrc(ds_auth.get("netrc_file")).authenticators(ds_auth["netrc_host"])
        creds = (username, password)
    return creds
