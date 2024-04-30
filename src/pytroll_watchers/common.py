"""Collection of function needed by multiple watchers."""

import datetime
import time


def run_every(interval):
    """Generator that ticks every `interval`.

    Args:
        interval: the timedelta object giving the amount of time to wait between ticks. An interval of 0 will just make
        tick once, then return (and thus busy loops aren't allowed).

    Yields:
        The time of the next tick.
    """
    while True:
        next_check = datetime.datetime.now(datetime.timezone.utc) + interval
        yield next_check
        to_wait = max(next_check.timestamp() - time.time(), 0)
        time.sleep(to_wait)
        if not interval:  # interval is 0
           break


def fromisoformat(datestring):
    """Wrapper around datetime's fromisoformat that also works on python 3.10."""
    try:
        return datetime.datetime.fromisoformat(datestring)
    except ValueError:
        # for python 3.10
        return datetime.datetime.strptime(datestring, "%Y-%m-%dT%H:%M:%S.%f%z")
