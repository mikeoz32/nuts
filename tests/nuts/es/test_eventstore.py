from dataclasses import dataclass
import pytest

from nuts.server.es.eventstore import (
    DomainEvent,
    NutsEventStore,
    StoredEvent,
    StreamReader,
    StreamWriter,
)
from nats import NATS

pytestmark = pytest.mark.asyncio


async def test_stream_reader_read(nc: NATS, test_stream: str):
    sr = StreamReader(nc, test_stream)
    sw = StreamWriter(nc, test_stream)

    await sw.write("1", b"test_message")
    await sw.write("1", b"test_message2")

    events = await sr.read_messages("1")
    assert len(events) == 2


async def test_stream_reader_read_all(nc: NATS, test_stream: str):
    sr = StreamReader(nc, test_stream)
    sw = StreamWriter(nc, test_stream)

    await sw.write("1", b"test_message")
    await sw.write("2", b"test_message2")

    events = await sr.read_messages("*")
    assert len(events) == 2


@dataclass
class ItemAddedToCart(DomainEvent):
    cartId: str
    itemName: str


async def test_event_store_publish_event(nc: NATS, test_stream: str):
    store = NutsEventStore(nc, test_stream)

    await store.publish(
        ItemAddedToCart(aggregate_id="cart-1", cartId="1", itemName="Item1")
    )

    events = await store.all()
    assert events
    for event in events:
        assert isinstance(event, StoredEvent)
        assert isinstance(event.payload, ItemAddedToCart)
        assert event.payload.aggregate_id == "cart-1"
        assert event.payload.cartId == "1"
