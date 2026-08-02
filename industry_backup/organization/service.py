import contextlib
from typing import Protocol

from nats import NATS
import nats

from nuts.nuts import Nuts
from nuts.server.nsgi import NsgiServer


class AggregateRepository:
    aggregate_name: str = "aggregate"

    def __init__(self, nc: NATS) -> None:
        self.nc = nc
        self.js = nc.jetstream()
        self.jsm = nc.jsm()

    async def get_by_id(self, aggregate_id: str):
        info = await self.jsm.stream_info(
            "nuts_aggregates", subjects_filter=f"{self.aggregate_name}.{aggregate_id}"
        )
        msgs_count = info.state.messages
        sub = await self.js.pull_subscribe(
            f"{self.aggregate_name}.{aggregate_id}", stream="nuts_aggregates"
        )

        msgs = await sub.fetch(msgs_count)
        return msgs


class DomainEvent:
    aggregate_name = "aggregate"
    event_name = "domain_event"

    def __init__(self, aggregate_id: str, payload) -> None:
        self._aggregate_id = aggregate_id
        self._payload = payload


class StreamReader:
    def __init__(self, nc: NATS, stream: str) -> None:
        self.js = nc.jetstream()
        self.jsm = nc.jsm()
        self._stream = stream

    async def read_messages(self):
        info = await self.jsm.stream_info(
            "nuts_aggregates", subjects_filter=f"{self.aggregate_name}.{aggregate_id}"
        )
        msgs_count = info.state.messages
        sub = await self.js.pull_subscribe(
            f"{self.aggregate_name}.{aggregate_id}", stream="nuts_aggregates"
        )

        msgs = await sub.fetch(msgs_count)
        return msgs

    async def _stream_info(self):
        self.jsm.add_stream


class EventPublisher:
    def __init__(self, nc: NATS) -> None:
        self.nc = nc
        self.js = nc.jetstream()

    async def publish(self, event: DomainEvent):
        self.js.publish(f"{event.aggregate_name}.{event._aggregate_id}", event._payload)


@contextlib.asynccontextmanager
async def ls():
    nc = await nats.connect("tls://localhost:4222")
    repo = AggregateRepository(nc)
    yield {"repo": repo}
    await nc.close()


service = Nuts("organization", lifespan=ls)


async def create_organization(repo: AggregateRepository):
    return "OK"


service.add_method("create_organization", create_organization)


class OrganizationServiceProto(Protocol):
    async def create_organization(self): ...


if __name__ == "__main__":
    NsgiServer("tls://localhost:4222").run(service)
