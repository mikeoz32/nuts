import json

import pytest

from nuts.proxy import AggregateProxy, RpcProxy

pytestmark = pytest.mark.asyncio


class FakeResponse:
    def __init__(self, data, headers=None):
        self.data = data
        self.headers = headers or {}


class FakeNc:
    def __init__(self):
        self.requests = []

    async def request(self, subject, payload, timeout=0):
        self.requests.append((subject, payload))
        return FakeResponse(
            data=json.dumps({"ok": True}).encode("utf-8"),
            headers={"status": "200"},
        )


async def test_rpc_proxy_calls_method():
    nc = FakeNc()
    proxy = RpcProxy("nats://localhost:4223", "svc", nc=nc)

    result = await proxy.sum(1, 2)

    assert result == {"ok": True}
    subject, payload = nc.requests[0]
    assert subject == "svc.rpc.sum"
    assert json.loads(payload) == {"args": [1, 2], "kwargs": {}}


async def test_aggregate_proxy_command():
    nc = FakeNc()
    proxy = AggregateProxy("nats://localhost:4223", "svc", nc=nc)

    await proxy.command(
        {
            "aggregate_type": "orders",
            "aggregate_id": "order-1",
            "command": "Create",
            "payload": {"value": 1},
            "metadata": {"trace_id": "abc"},
        }
    )

    subject, payload = nc.requests[0]
    assert subject == "svc.es.command.orders"
    message = json.loads(payload)
    assert message["aggregate_id"] == "order-1"
    assert message["command"] == "Create"
