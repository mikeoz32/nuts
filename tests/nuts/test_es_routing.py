import pytest

from nuts.nuts import Nuts

pytestmark = pytest.mark.asyncio


async def test_es_command_route_handles_command():
    app = Nuts("test")

    async def handle_create(payload, aggregate, metadata=None):
        assert aggregate is None
        assert metadata == {"trace_id": "abc", "aggregate_id": "order-1"}
        return [{"name": "Created", "payload": {"value": payload["value"]}}]

    app.add_command("orders", "Create", handle_create)

    scope = {
        "type": "es",
        "aggregate_type": "orders",
        "aggregate_id": "order-1",
        "state": {},
    }

    request = {
        "type": "es.command.request",
        "command": "Create",
        "payload": {"value": 42},
        "aggregate": None,
        "metadata": {"trace_id": "abc"},
    }

    responses = []

    async def receive():
        return request

    async def send(message):
        responses.append(message)

    await app(scope, receive, send)

    assert responses == [
        {
            "type": "es.command.response",
            "events": [{"name": "Created", "payload": {"value": 42}}],
        }
    ]


async def test_es_event_route_applies_event():
    app = Nuts("test")

    async def apply_created(payload, aggregate):
        state = aggregate or {}
        state.update({"value": payload["value"]})
        return state

    app.add_aggregate_event("orders", "Created", apply_created)

    scope = {
        "type": "es",
        "aggregate_type": "orders",
        "aggregate_id": "order-1",
        "state": {},
    }

    request = {
        "type": "es.event.request",
        "event": "Created",
        "payload": {"value": 10},
        "aggregate": None,
    }

    responses = []

    async def receive():
        return request

    async def send(message):
        responses.append(message)

    await app(scope, receive, send)

    assert responses == [{"type": "es.event.response", "aggregate": {"value": 10}}]
