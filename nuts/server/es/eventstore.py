from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
import json
from typing import Dict, Generic, List, Protocol, TypeVar
from uuid import UUID
from nats import NATS
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig, DeliverPolicy

from nuts.server.importtools import create_class, fqn

DEP = TypeVar("_DEP")


@dataclass
class DomainEvent(Generic[DEP]):
    aggregate_id: str


@dataclass
class StoredEvent:
    payload: DomainEvent
    event_type: str
    sequence: int = 0

    def from_dict(data: Dict):
        print(data)
        event_type = data.get("event_type")
        event_cls = create_class(event_type)
        return StoredEvent(payload=event_cls(**data["payload"]), event_type=event_type)


def make_subject(aggregate_id: str) -> str:
    return f"aggregate.{aggregate_id}"


def deliver_policy_for_seq(seq: int) -> DeliverPolicy:
    return DeliverPolicy.BY_START_SEQUENCE if seq else DeliverPolicy.ALL


class StreamReader:
    def __init__(self, nc: NATS, stream: str) -> None:
        self.js = nc.jetstream()
        self.jsm = nc.jsm()
        self._stream = stream

    async def read_messages(self, aggregate_id: str, seq: int = 0):
        subj = make_subject(aggregate_id)

        msgs_count = await self.count_messages(subj) - seq

        if msgs_count <= 0:
            return []

        async with self.pull_subscribe(subj, seq) as sub:
            return await sub.fetch(msgs_count)

    async def count_messages(self, subject: str):
        info = await self.jsm.stream_info(self._stream, subjects_filter=subject)
        return info.state.messages

    @asynccontextmanager
    async def pull_subscribe(self, subj: str, seq: int = 0):
        sub = await self.js.pull_subscribe(
            subj,
            stream=self._stream,
            config=ConsumerConfig(
                opt_start_seq=seq, deliver_policy=deliver_policy_for_seq(seq)
            ),
        )

        yield sub

        await sub.unsubscribe()

    async def _stream_info(self):
        self.jsm.add_stream


class StreamWriter:
    def __init__(self, nc: NATS, stream: str) -> None:
        self.js = nc.jetstream()
        self._stream = stream

    async def write(self, aggregate_id: str, data: bytes):
        await self.js.publish(make_subject(aggregate_id), data, stream=self._stream)


class SerializerProtocol(Protocol):
    def serialize(self, event: StoredEvent) -> bytes: ...
    def deserialize(self, data: bytes) -> StoredEvent: ...


class JsonSerializer(SerializerProtocol):
    def serialize(self, event: StoredEvent) -> bytes:
        return json.dumps(asdict(event)).encode("utf-8")

    def deserialize(self, data: bytes) -> StoredEvent:
        return json.loads(data)


class NutsEventStore:
    def __init__(self, nc: NATS, name: str) -> None:
        self.reader = StreamReader(nc, name)
        self.writer = StreamWriter(nc, name)
        self.serializer: SerializerProtocol = JsonSerializer()

    async def publish(self, event: DomainEvent):
        aggregate_id = event.aggregate_id
        stored_event = StoredEvent(payload=event, event_type=fqn(event))
        data = self.serializer.serialize(stored_event)
        await self.writer.write(aggregate_id, data)

    async def all(self):
        return self.process_messages(await self.reader.read_messages("*"))

    async def read(self, aggregate_id: str, seq: int = 0):
        return await self.reader.read_messages(aggregate_id, seq)

    def process_messages(self, messages: List[Msg]):
        return list(map(self.process_message, messages))

    def process_message(self, message: Msg):
        data = self.serializer.deserialize(message.data)
        return StoredEvent.from_dict(data)
