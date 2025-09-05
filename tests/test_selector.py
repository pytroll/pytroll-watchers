"""Tests for the selector."""
import time

import yaml
from posttroll.message import Message
from posttroll.testing import patched_publisher, patched_subscriber_recv

from pytroll_watchers.selector import (
    TTLDict,
    _run_selector_with_managed_dict_server,
    cli,
    run_selector,
)


def test_run_selector(tmp_path):
    """Test running a selector."""
    uid = "IVCDB_j03_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg1 = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
            '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    messages = [Message.decode(msg1)]

    pipe_in_address = "ipc://" + str(tmp_path / "in.ipc")
    pipe_out_address = "ipc://" + str(tmp_path / "out.ipc")
    subscriber_config = dict(addresses=[pipe_in_address],
                             nameserver=False,
                             port=3000)

    publisher_config = dict(address=pipe_out_address,
                            nameservers=False,
                            port=1999)

    selector_config = dict(ttl=0.1)

    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            run_selector(selector_config, subscriber_config, publisher_config)
    assert len(published_messages) == 1


def test_run_selector_ignores_non_file_messages(tmp_path):
    """Test running a selector ignore irrelevant messages."""
    uid = "IVCDB_j04_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg1 = ('pytroll://segment/viirs/l1b/ del a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
            '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    messages = [Message.decode(msg1)]

    pipe_in_address = "ipc://" + str(tmp_path / "in.ipc")
    pipe_out_address = "ipc://" + str(tmp_path / "out.ipc")
    subscriber_config = dict(addresses=[pipe_in_address],
                             nameserver=False,
                             port=3000)

    publisher_config = dict(address=pipe_out_address,
                            nameservers=False,
                            port=2000)

    selector_config = dict(ttl=0.1)

    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            run_selector(selector_config, subscriber_config, publisher_config)
    assert len(published_messages) == 0


def create_data_file(path):
    """Create a data file."""
    path.parent.mkdir(exist_ok=True)

    with open(path, "w") as fd:
        fd.write("data")


def test_run_selector_on_single_file_messages(tmp_path):
    """Test running the selector on single file messages."""
    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    uid2 = "IVCDB_j01_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file2 = tmp_path / "sdr" / uid2
    create_data_file(sdr_file2)


    msg1 = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
            '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    msg2 = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid}", "uri": "ssh://someplace.pytroll.org:/{str(sdr_file)}", "path": "{str(sdr_file)}", '
            '"filesystem": {"cls": "fsspec.implementations.ssh.SFTPFileSystem", "protocol": "ssh", "args": []}}')

    msg3 = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid2}", "uri": "ssh://someplace.pytroll.org:/{str(sdr_file2)}", "path": "{str(sdr_file2)}", '
            '"filesystem": {"cls": "fsspec.implementations.ssh.SFTPFileSystem", "protocol": "ssh", "args": []}}')

    messages = [Message.decode(msg1), Message.decode(msg2), Message.decode(msg3)]

    pipe_in_address = "ipc://" + str(tmp_path / "in.ipc")
    pipe_out_address = "ipc://" + str(tmp_path / "out.ipc")
    subscriber_config = dict(addresses=[pipe_in_address],
                             nameserver=False,
                             port=3000)

    publisher_config = dict(address=pipe_out_address,
                            port=1999,
                            nameservers=False)

    selector_config = dict(ttl=0.1)

    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            _run_selector_with_managed_dict_server(selector_config, subscriber_config, publisher_config)
    assert len(published_messages) == 2
    assert published_messages[0] == msg1
    assert published_messages[1] == msg3


def test_ttldict():
    """Test the TTLDict."""
    ttl = 0.1
    key = "uid_1"
    value = b"some stuff"
    other_value = b"some other important stuff"

    sel = TTLDict(ttl)

    sel[key] = value
    assert sel[key] == value
    sel[key] = other_value
    assert sel[key] == value
    time.sleep(ttl + 0.1)
    assert key not in sel
    sel[key] = other_value
    assert sel[key] == other_value


def test_cli(tmp_path):
    """Test the command-line interface."""
    uid = "IVCDB_j03_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg1 = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
            'application/json {"sensor": "viirs", '
            f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
            '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    messages = [Message.decode(msg1)]

    pipe_in_address = "ipc://" + str(tmp_path / "in.ipc")
    pipe_out_address = "ipc://" + str(tmp_path / "out.ipc")
    subscriber_config = dict(addresses=[pipe_in_address],
                             nameserver=False,
                             port=3000)

    publisher_config = dict(address=pipe_out_address,
                            nameservers=False,
                            port=1999)

    selector_config = dict(ttl=0.1)
    config = dict(publisher_config=publisher_config,
                  subscriber_config=subscriber_config,
                  selector_config=selector_config)
    config_file = tmp_path / "selector_config"
    with open(config_file, "w") as fd:
        fd.write(yaml.dump(config))
    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            cli([str(config_file)])
    assert len(published_messages) == 1
