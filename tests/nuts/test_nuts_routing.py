import contextlib

import pytest

from nuts.nuts import Nuts

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def lifespan_context():
    yield {"db": "ready"}


async def test_lifespan_flow():
    app = Nuts("svc", lifespan=lifespan_context)

    scope = {"type": "lifespan", "state": {}, "app_info": {}}
    messages = []

    async def receive():
        return {}

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)

    assert messages == [
        {"type": "lifespan.startup.complete"},
        {"type": "lifespan.shutdown.complete"},
    ]
    assert scope["state"]["db"] == "ready"
    assert scope["app_info"]["name"] == "svc"


async def test_rpc_route_success():
    app = Nuts("svc")

    async def handler(x, y):
        return x + y

    app.add_method("sum", handler)

    scope = {"type": "rpc", "method": "sum", "state": {}}
    request = {"args": [1, 2], "kwargs": {}}
    messages = []

    async def receive():
        return request

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)

    assert messages == [
        {"type": "rpc.reponse", "body": 3, "headers": {"status": "200"}}
    ]


async def test_rpc_route_not_found():
    app = Nuts("svc")

    scope = {"type": "rpc", "method": "missing", "state": {}}
    request = {"args": [], "kwargs": {}}
    messages = []

    async def receive():
        return request

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)

    assert messages == [
        {"type": "rpc.reponse", "body": "Not found", "headers": {"status": "404"}}
    ]
