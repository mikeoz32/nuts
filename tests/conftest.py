import nats
from nats.js.api import StreamConfig
import pytest_asyncio

from industry.organization.service import EventPublisher


@pytest_asyncio.fixture(scope="function")
async def nc():
    nc = await nats.connect("tls://localhost:4222")
    yield nc
    await nc.close()


@pytest_asyncio.fixture(scope="function")
async def test_stream(nc) -> str:
    STREAM_NAME = "test_stream"

    jsm = nc.jsm()
    try:
        await jsm.delete_stream(STREAM_NAME)
    except BaseException:
        pass

    await jsm.add_stream(StreamConfig(name=STREAM_NAME, subjects=["aggregate.*"]))

    yield STREAM_NAME

    await jsm.delete_stream(STREAM_NAME)


@pytest_asyncio.fixture(scope="function")
async def event_publisher(nc):
    yield EventPublisher(nc)
