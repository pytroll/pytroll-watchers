"""Test the common utilities."""

from datetime import timedelta
from signal import SIGTERM
from threading import Thread
from time import sleep

from pytroll_watchers.common import TERM_EVENT, get_oauth_credentials, handle_sigterm, run_every


def test_run_every_terminates():
    """Test that run_every terminates."""
    res = []
    def runner():
        for _ in run_every(timedelta(seconds=1)):
            res.append("test")
    thr = Thread(target=runner)
    thr.start()
    sleep(.01)
    TERM_EVENT.set()
    thr.join()
    assert len(res) == 1
    TERM_EVENT.clear()


def test_run_every_sigterm():
    """Test that run_every terminates on sigterm."""
    res = []
    def runner():
        for _ in run_every(timedelta(seconds=1)):
            res.append("test")
    thr = Thread(target=runner)
    thr.start()
    sleep(.01)
    handle_sigterm(SIGTERM, None)
    thr.join()
    assert len(res) == 1
    TERM_EVENT.clear()


def test_get_oauth_credentials_use_env(monkeypatch):
    """Test that credentials can be passed through the env."""
    user = "user1"
    passwd = "pass2"  # noqa
    monkeypatch.setenv("OAUTH_USERNAME", user)
    monkeypatch.setenv("OAUTH_PASSWORD", passwd)
    assert get_oauth_credentials(dict()) == (user, passwd)
