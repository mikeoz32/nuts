import asyncio
from logging import getLogger
from typing import Any

logger = getLogger("nuts.server.message_handler")


class MessageHandler:
    request_type: str
    response_type: str

    def __init__(self, app, nc, state) -> None:
        self.app = app
        self.nc = nc
        self.state = state
        self.request_sent = asyncio.Event()
        self.request_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.response_received = asyncio.Event()

    def scope_from_message(self, message):
        # method: str = message.subject
        # method = method.split(".")[1]
        # return {
        #     "type": "rpc",
        #     "headers": message.headers or {"content-type": "application/json"},
        #     "method": method,
        # }
        return  {
            "headers": message.headers or {"content-type": "application/json"}
        }
    async def __call__(self, msg) -> Any:
        logger.debug(f"Received message: {msg}")

        scope = self.scope_from_message(message=msg)
        scope["state"] = self.state

        serializer = SERIALIZERS_MAP.get(
            scope["headers"]["content-type"], "application/json"
        )
        parameters = serializer.deserialize_rpc_request(msg.data)
        assert isinstance(parameters, dict)
        assert isinstance(parameters.get("args", []), list)
        assert isinstance(parameters.get("kwargs", {}), dict)

        request_message = {"type": self.request_type, "body": msg.data}
        request_message.update(parameters)

        await self.request_queue.put(request_message)

        async def wrap_app():
            try:
                await self.app(scope, self.receive, self.send)
            except BaseException as ex:
                logger.error(ex)
                await self.response_queue.put({"body": str(ex)})
                self.response_received.set()

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
