"""Collection of function needed by multiple watchers."""

import datetime
import netrc
import signal
import time
from collections.abc import Generator
from logging import getLogger
from os import environ
from threading import Event

logger = getLogger(__name__)

TERM_EVENT = Event()


def handle_sigterm(signum, frame):
    """Handle sigterm."""
    logger.info(f"Received SIGTERM ({signum}). Cleaning up...")
    TERM_EVENT.set()


signal.signal(signal.SIGTERM, handle_sigterm)


def run_every(interval: datetime.timedelta) -> Generator[datetime.datetime]:
    """Generator that ticks every `interval`.

    Args:
        interval: the timedelta object giving the amount of time to wait between ticks. An interval of 0 will just make
        tick once, then return (and thus busy loops aren't allowed). Can be interrupting by setting TERM_EVENT or by
        sending SIGTERM.

    Yields:
        The time of the next tick.
    """
    next_check = datetime.datetime.now(datetime.timezone.utc)
    while not TERM_EVENT.is_set():
        next_check += interval
        yield next_check
        to_wait = max(next_check.timestamp() - time.time(), 0)
        to_be_continued = TERM_EVENT.wait(to_wait)
        if not interval and not to_be_continued:  # interval is 0
           break


def fromisoformat(datestring):
    """Wrapper around datetime's fromisoformat that also works on python 3.10."""
    try:
        return datetime.datetime.fromisoformat(datestring)
    except ValueError:
        # for python 3.10
        return datetime.datetime.strptime(datestring, "%Y-%m-%dT%H:%M:%S.%f%z")


def get_oauth_credentials(ds_auth: dict[str, str]) -> tuple[str, str]:
    """Get credentials from the ds_auth dictionary or the enviromnent."""
    try:
        try:
            creds = ds_auth["username"], ds_auth["password"]
        except KeyError:
            creds = environ["OAUTH_USERNAME"], environ["OAUTH_PASSWORD"]
    except KeyError:
        username, _, password = netrc.netrc(ds_auth.get("netrc_file")).authenticators(ds_auth["netrc_host"])
        creds = (username, password)
    return creds
