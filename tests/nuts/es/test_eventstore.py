import pytest

from nuts.server.es.eventstore import (
    ConcurrencyError,
    EventStore,
    StreamReader,
    StreamWriter,
    make_subject,
)
from nats import NATS

pytestmark = pytest.mark.asyncio


async def test_stream_reader_read(nc: NATS, test_stream: str):
    sr = StreamReader(nc, test_stream)
    sw = StreamWriter(nc, test_stream)
    subject = make_subject("test", "cart", "1")

    await sw.write(subject, b"test_message")
    await sw.write(subject, b"test_message2")

    events = await sr.read_messages(subject)
    assert len(events) == 2


async def test_stream_reader_read_all(nc: NATS, test_stream: str):
    sr = StreamReader(nc, test_stream)
    sw = StreamWriter(nc, test_stream)
    subject_1 = make_subject("test", "cart", "1")
    subject_2 = make_subject("test", "cart", "2")

    await sw.write(subject_1, b"test_message")
    await sw.write(subject_2, b"test_message2")

    events = await sr.read_messages("test.es.event.>")
    assert len(events) == 2


async def test_event_store_append_and_load(nc: NATS, test_stream: str):
    store = EventStore(nc, test_stream, app_name="test")

    stored = await store.append(
        "cart",
        "cart-1",
        expected_version=0,
        events=[{"name": "ItemAdded", "payload": {"cartId": "1", "itemName": "Item1"}}],
    )

    assert stored[0].aggregate_id == "cart-1"
    assert stored[0].sequence == 1

    events = await store.load("cart", "cart-1")
    assert len(events) == 1
    assert events[0].name == "ItemAdded"
    assert events[0].payload["cartId"] == "1"
    assert await store.current_version("cart", "cart-1") == 1


async def test_event_store_conflict(nc: NATS, test_stream: str):
    store = EventStore(nc, test_stream, app_name="test")

    await store.append(
        "cart",
        "cart-1",
        expected_version=0,
        events=[{"name": "ItemAdded", "payload": {"cartId": "1"}}],
    )

    with pytest.raises(ConcurrencyError):
        await store.append(
            "cart",
            "cart-1",
            expected_version=0,
            events=[{"name": "ItemAdded", "payload": {"cartId": "2"}}],
        )


async def test_event_store_load_from_sequence(nc: NATS, test_stream: str):
    store = EventStore(nc, test_stream, app_name="test")

    await store.append(
        "cart",
        "cart-1",
        expected_version=0,
        events=[
            {"name": "ItemAdded", "payload": {"cartId": "1", "item": "A"}},
            {"name": "ItemAdded", "payload": {"cartId": "1", "item": "B"}},
        ],
    )

    events = await store.load("cart", "cart-1", from_sequence=1)
    assert len(events) == 1
    assert events[0].payload["item"] == "B"
