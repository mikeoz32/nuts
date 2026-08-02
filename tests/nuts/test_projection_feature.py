import pytest

from nuts.server.es.eventstore import EventEnvelope
from nuts.server.projections.feature import ProjectionMessageHandler

pytestmark = pytest.mark.asyncio


class FakeStore:
    def __init__(self, record=None):
        self.record = record
        self.saved = []

    async def load(self, projection_name, aggregate_type, aggregate_id):
        return self.record

    async def save(
        self, projection_name, aggregate_type, aggregate_id, state, last_sequence
    ):
        self.saved.append((projection_name, aggregate_type, aggregate_id, state, last_sequence))
        return 1


class FakeRecord:
    def __init__(self, state, last_sequence):
        self.state = state
        self.last_sequence = last_sequence


class FakeRoute:
    def __init__(self, projection_name, aggregate_type, event_name):
        self.projection_name = projection_name
        self.aggregate_type = aggregate_type
        self.event_name = event_name


async def test_projection_handler_skips_old_sequence():
    async def app(scope, receive, send):
        await send({"type": "projection.event.response", "projection": {"count": 2}})

    store = FakeStore(record=FakeRecord(state={"count": 1}, last_sequence=5))
    handler = ProjectionMessageHandler(app, None, {}, store, timeout=1.0)
    route = FakeRoute("order_totals", "orders", "Created")
    event = EventEnvelope(
        aggregate_id="1",
        aggregate_type="orders",
        name="Created",
        payload={"value": 1},
        sequence=4,
        metadata={},
    )

    await handler.apply_route(route, event)

    assert store.saved == []


async def test_projection_handler_updates_on_new_sequence():
    async def app(scope, receive, send):
        await send({"type": "projection.event.response", "projection": {"count": 2}})

    store = FakeStore(record=FakeRecord(state={"count": 1}, last_sequence=5))
    handler = ProjectionMessageHandler(app, None, {}, store, timeout=1.0)
    route = FakeRoute("order_totals", "orders", "Created")
    event = EventEnvelope(
        aggregate_id="1",
        aggregate_type="orders",
        name="Created",
        payload={"value": 1},
        sequence=6,
        metadata={"trace": "t1"},
    )

    await handler.apply_route(route, event)

    assert store.saved == [
        ("order_totals", "orders", "1", {"count": 2}, 6)
    ]
