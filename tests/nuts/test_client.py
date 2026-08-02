import json

import pytest

from nuts.client import NutsClient

pytestmark = pytest.mark.asyncio


class FakeResponse:
    def __init__(self, data, headers=None):
        self.data = data
        self.headers = headers or {}


class FakeNc:
    def __init__(self):
        self.requests = []
        self.closed = False

    async def request(self, subject, payload, timeout=0):
        self.requests.append((subject, payload))
        return FakeResponse(
            data=json.dumps({"ok": True}).encode("utf-8"),
            headers={"status": "200"},
        )

    async def close(self):
        self.closed = True


async def test_client_rpc_and_es_share_connection():
    nc = FakeNc()
    client = NutsClient("nats://localhost:4223", "svc", nc=nc)

    await client.rpc.sum(1, 2)
    await client.es.command(
        {
            "aggregate_type": "orders",
            "aggregate_id": "order-1",
            "command": "Create",
            "payload": {"value": 1},
        }
    )

    assert len(nc.requests) == 2
    assert nc.requests[0][0] == "svc.rpc.sum"
    assert nc.requests[1][0] == "svc.es.command.orders"


async def test_client_close():
    nc = FakeNc()
    client = NutsClient("nats://localhost:4223", "svc", nc=nc)

    await client.close()

    assert nc.closed is True
