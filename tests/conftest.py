import asyncio
from contextlib import suppress
import socket
import subprocess
import tempfile
import time

import nats
from nats.js.api import StreamConfig
import pytest
import pytest_asyncio

from industry.organization.service import EventPublisher


def _is_port_open(host: str, port: int) -> bool:
    with suppress(OSError), socket.create_connection((host, port), timeout=0.5):
        return True
    return False


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_port_open(host, port):
            return True
        time.sleep(0.1)
    return False


@pytest.fixture(scope="session", autouse=True)
def nats_server():
    host = "127.0.0.1"
    port = 4223

    process = None
    temp_dir = None
    if not _is_port_open(host, port):
        temp_dir = tempfile.TemporaryDirectory()
        process = subprocess.Popen(
            ["nats-server", "-js", "-p", str(port), "-sd", temp_dir.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not _wait_for_port(host, port):
            process.terminate()
            process.wait(timeout=5)
            temp_dir.cleanup()
            pytest.skip("NATS server did not start in time")

    yield

    if process:
        process.terminate()
        process.wait(timeout=5)
    if temp_dir:
        temp_dir.cleanup()


@pytest_asyncio.fixture(scope="function")
async def nc():
    try:
        nc = await asyncio.wait_for(
            nats.connect(
                "nats://localhost:4223",
                connect_timeout=1,
                max_reconnect_attempts=0,
            ),
            timeout=2,
        )
    except Exception as exc:
        pytest.skip(f"NATS not available: {exc}")
    try:
        js = nc.jetstream()
        for _ in range(10):
            try:
                await js.account_info()
                break
            except Exception:
                await asyncio.sleep(0.1)
        else:
            pytest.skip("JetStream not available")
        yield nc
    finally:
        await nc.close()


@pytest_asyncio.fixture(scope="function")
async def test_stream(nc) -> str:
    STREAM_NAME = "test_stream"

    jsm = nc.jsm()
    try:
        await jsm.delete_stream(STREAM_NAME)
    except BaseException:
        pass

    await jsm.add_stream(StreamConfig(name=STREAM_NAME, subjects=["test.es.event.>"]))

    yield STREAM_NAME

    await jsm.delete_stream(STREAM_NAME)


@pytest_asyncio.fixture(scope="function")
async def event_publisher(nc):
    yield EventPublisher(nc)
