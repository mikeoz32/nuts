"""
# RPC Feature

add support for calling service methods


## Specification
scope['type'] = "rpc"

'rpc.request' - receive message
Send by an application when got request on method call
message['method'] = method name
message['headers'] = request headers

'rpc.response' - send message
message['body'] = bytes of data


"""

import asyncio
import json
from logging import getLogger
from typing import Any, Dict, Protocol
import nats

from nuts.server.feature import ServerFeature

logger = getLogger("nuts.server.rpc")


def scope_from_message(message):
    method: str = message.subject
    method = method.split(".")[1]
    return {
        "type": "rpc",
        "headers": message.headers or {"content-type": "application/json"},
        "method": method,
    }


class SerializerProtocol(Protocol):
    def deserialize_rpc_request(self, raw): ...
    def serialize_rpc_response(self, raw): ...


class JsonSerializer(SerializerProtocol):
    def deserialize_rpc_request(self, raw):
        data = json.loads(raw)

        return data

    def serialize_rpc_response(self, raw):
        b = json.dumps(raw)

        return bytes(b, "utf-8")


SERIALIZERS_MAP: Dict[str, SerializerProtocol] = {"application/json": JsonSerializer()}


class RpcMessageHandler:
    def __init__(self, app, nc, state) -> None:
        self.app = app
        self.nc = nc
        self.state = state
        self.request_sent = asyncio.Event()
        self.request_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.response_received = asyncio.Event()

    async def __call__(self, msg) -> Any:
        logger.debug(f"Received message: {msg}")

        scope = scope_from_message(message=msg)
        scope["state"] = self.state

        serializer = SERIALIZERS_MAP.get(
            scope["headers"]["content-type"], "application/json"
        )
        parameters = serializer.deserialize_rpc_request(msg.data)
        assert isinstance(parameters, dict)
        assert isinstance(parameters.get("args", []), list)
        assert isinstance(parameters.get("kwargs", {}), dict)

        request_message = {"type": "rpc.request", "body": msg.data}
        request_message.update(parameters)

        await self.request_queue.put(request_message)

        async def wrap_app():
            try:
                await self.app(scope, self.receive, self.send)
            except BaseException as ex:
                logger.error(ex)
                await self.response_queue.put({"body": str(ex)})
                self.response_received.set()

        # request_task = asyncio.create_task(self.app(scope, self.receive, self.send))  # noqa
        request_task = asyncio.create_task(wrap_app())  # noqa

        try:
            async with asyncio.timeout(1):
                await self.response_received.wait()
        except asyncio.TimeoutError:
            request_task.cancel()
            await self.nc.publish(msg.reply, b"", headers={"status": "timeout"})

        response = await self.response_queue.get()
        logger.debug(f" Response: {response}")

        request_task.cancel()

        await self.nc.publish(
            msg.reply,
            serializer.serialize_rpc_response(response.get("body")),
            headers={"status": "200"},
        )

    async def send(self, message):
        await self.response_queue.put(message)
        self.response_received.set()

    async def receive(self):
        return await self.request_queue.get()


class RpcFeature(ServerFeature):
    def __init__(self, server) -> None:
        self.server = server
        self.nc = None
        self.app = server.app
        self.requests = set()

    async def startup(self):
        logger.info("Starting RPC feature")
        self.nc = await nats.connect(self.server._nats_url)

        async def handle_message(msg):
            handler = RpcMessageHandler(
                self.server.app, self.nc, self.server.lifespan.state
            )
            request_task = asyncio.create_task(handler(msg))
            request_task.add_done_callback(self.requests.discard)

        await self.nc.subscribe(
            f"{self.server.lifespan.app_name}.rpc.*",
            self.server.lifespan.app_name,
            handle_message,
        )

    async def on_tick(self, counter):
        if counter % 1000 == 1:
            print(self.requests)

    async def shutdown(self):
        await self.nc.drain()
        await self.nc.close()
        logger.info("Shutting down RPC")
