from __future__ import annotations

from typing import Any

from nuts.proxy import AggregateProxy, RpcProxy


class NutsClient:
    def __init__(self, nats_url: str, service: str, nc: Any | None = None) -> None:
        self.rpc = RpcProxy(nats_url, service, nc=nc)
        self.es = AggregateProxy(nats_url, service, nc=nc)

    async def close(self) -> None:
        nc = getattr(self.rpc, "_nc", None)
        if nc and hasattr(nc, "close"):
            await nc.close()
