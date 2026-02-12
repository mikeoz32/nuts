import pytest

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts

pytestmark = pytest.mark.asyncio


class OrderAggregate(Aggregate):
    aggregate_type = "orders"

    @Command("Create")
    def create(self, payload, metadata=None):
        assert metadata["aggregate_id"] == "order-1"
        return [Event("Created", {"value": payload["value"]})]

    @Event("Created")
    def apply_created(self, payload):
        self.state["value"] = payload["value"]


async def test_register_aggregate_routes_command_and_event():
    app = Nuts("test")
    app.add_aggregate(OrderAggregate)

    scope = {
        "type": "es",
        "aggregate_type": "orders",
        "aggregate_id": "order-1",
        "state": {},
    }

    responses = []

    async def send(message):
        responses.append(message)

    async def receive_command():
        return {
            "type": "es.command.request",
            "command": "Create",
            "payload": {"value": 42},
            "aggregate": None,
            "metadata": {"trace_id": "abc"},
        }

    await app(scope, receive_command, send)

    assert responses == [
        {
            "type": "es.command.response",
            "events": [{"name": "Created", "payload": {"value": 42}}],
        }
    ]

    responses.clear()

    async def receive_event():
        return {
            "type": "es.event.request",
            "event": "Created",
            "payload": {"value": 42},
            "aggregate": None,
        }

    await app(scope, receive_event, send)

    assert responses == [
        {"type": "es.event.response", "aggregate": {"value": 42}}
    ]
