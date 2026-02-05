import json

import pytest

from nuts.server.es.eventstore import ConcurrencyError, EventEnvelope
from nuts.server.es.feature import EsMessageHandler

pytestmark = pytest.mark.asyncio


class FakeEventStore:
    def __init__(self):
        self.append_calls = 0

    async def load(self, aggregate_type, aggregate_id, *, from_sequence=0):
        return []

    async def append(
        self,
        aggregate_type,
        aggregate_id,
        *,
        expected_version,
        events,
        metadata=None,
    ):
        self.append_calls += 1
        if self.append_calls == 1:
            raise ConcurrencyError("conflict")
        return [
            EventEnvelope(
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                name=events[0]["name"],
                payload=events[0]["payload"],
                sequence=1,
                metadata=metadata,
            )
        ]


class FakeSnapshotStore:
    async def load(self, aggregate_type, aggregate_id):
        return None

    async def save(self, aggregate_type, aggregate_id, state, last_sequence):
        return 1


class FakeNc:
    def __init__(self):
        self.published = []

    async def publish(self, reply, payload):
        self.published.append((reply, payload))


class FakeMsg:
    def __init__(self, data):
        self.data = data
        self.reply = "reply"


async def test_es_conflict_retries_then_succeeds():
    async def app(scope, receive, send):
        request = await receive()
        if request["type"] == "es.command.request":
            await send(
                {
                    "type": "es.command.response",
                    "events": [{"name": "Created", "payload": {"value": 1}}],
                }
            )
        elif request["type"] == "es.event.request":
            await send({"type": "es.event.response", "aggregate": {"value": 1}})

    nc = FakeNc()
    handler = EsMessageHandler(
        app,
        nc,
        {},
        FakeEventStore(),
        FakeSnapshotStore(),
        timeout=1.0,
        snapshot_interval=0,
        conflict_retries=1,
        conflict_backoff=0.0,
    )

    command = {
        "aggregate_type": "orders",
        "aggregate_id": "order-1",
        "command": "Create",
        "payload": {"value": 1},
        "metadata": {},
    }

    msg = FakeMsg(json.dumps(command).encode("utf-8"))
    await handler(msg)

    assert handler.event_store.append_calls == 2
    assert len(nc.published) == 1
    _, payload = nc.published[0]
    response = json.loads(payload)
    assert response["type"] == "es.command.response"
