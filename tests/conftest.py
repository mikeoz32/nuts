import asyncio
from contextlib import suppress
from pathlib import Path
import socket
import time
from typing import AsyncGenerator

import nats
from nats.js.api import StreamConfig
import pytest
import pytest_asyncio

from industry_backup.organization.service import EventPublisher


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


@pytest.fixture(scope="session")
def docker_compose_file() -> list[str]:
    return [str(Path(__file__).with_name("docker-compose.yml"))]


@pytest.fixture(scope="session")
def nats_server(docker_services):
    host = "127.0.0.1"
    port = 4223
    if _is_port_open(host, port):
        return True

    try:
        docker_services.wait_until_responsive(
            timeout=25.0,
            pause=0.2,
            check=lambda: _is_port_open(host, port),
        )
    except Exception:
        return False

    return _wait_for_port(host, port, timeout=5.0)


@pytest_asyncio.fixture(scope="function")
async def nc(nats_server) -> AsyncGenerator:
    if not nats_server:
        pytest.skip("NATS with JetStream is not available for integration tests")

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
        return
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
async def test_stream(nc) -> AsyncGenerator[str, None]:
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
