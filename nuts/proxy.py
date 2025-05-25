from dataclasses import asdict
from logging import getLogger
from typing import Any, Callable, Dict
import json
import nats

from nuts.server.es.feature import CommandMessage

rpclog = getLogger("nuts.proxy.rpc")
agglog = getLogger("nuts.proxy.aggregate")


class RpcProxy:
    def __init__(self, nats_url, service_name: str, nc=None) -> None:
        self._nats_url = nats_url
        self._nc = nc
        self.service_name = service_name

    async def get_connection(self):
        if self._nc is None:
            self._nc = await nats.connect(self._nats_url)
            print("Connected")
        return self._nc

    def __getattr__(self, name: str) -> Callable:
        async def proxy(*args, **kwargs):
            nc = await self.get_connection()
            data = json.dumps({"args": args, "kwargs": kwargs})
            response = await nc.request(
                f"{self.service_name}.rpc.{name}", bytes(data, "utf-8"), timeout=3
            )
            status = response.headers.get("status", 200)
            body = json.loads(response.data)
            if int(status) >= 400:
                raise Exception(body)
            return body

        return proxy


class AggregateProxy:
    def __init__(self, nats_url, service_name: str, nc=None) -> None:
        self._nats_url = nats_url
        self._nc = nc
        self.service_name = service_name

    async def get_connection(self):
        if self._nc is None:
            self._nc = await nats.connect(self._nats_url)
            print("Connected")
        return self._nc

    async def command(self, command: Dict[str, Any]):
        nc = await self.get_connection()

        payload_fqn = command.__class__.__module__ + "." + command.__class__.__name__

        message = CommandMessage(
            aggregate_id="1", payload=command, command_class=payload_fqn
        )

        data = json.dumps(asdict(message))
        response = await nc.request(
            f"{self.service_name}.es.command.{command.__class__.__name__}",
            bytes(data, "utf-8"),
        )
        agglog.info(response)
