from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
import json
from typing import Any, Dict, Iterable, List, Protocol

from nats import NATS
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig, DeliverPolicy, RetentionPolicy, StreamConfig
from nats.js.errors import NotFoundError


class ConcurrencyError(RuntimeError):
    pass


@dataclass
class EventEnvelope:
    aggregate_id: str
    aggregate_type: str
    name: str
    payload: Dict[str, Any]
    sequence: int
    metadata: Dict[str, Any] | None = None


def make_subject(app: str, aggregate_type: str, aggregate_id: str) -> str:
    return f"{app}.es.event.{aggregate_type}.{aggregate_id}"


def deliver_policy_for_seq(seq: int) -> DeliverPolicy:
    return DeliverPolicy.BY_START_SEQUENCE if seq else DeliverPolicy.ALL


class StreamReader:
    def __init__(self, nc: NATS, stream: str) -> None:
        self.js = nc.jetstream()
        self.jsm = nc.jsm()
        self._stream = stream

    async def read_messages(self, subject: str, stream_seq: int = 0):
        msgs_count = await self.count_messages(subject)

        if msgs_count <= 0:
            return []

        async with self.pull_subscribe(subject, stream_seq) as sub:
            return await sub.fetch(msgs_count)

    async def count_messages(self, subject: str):
        info = await self.jsm.stream_info(self._stream, subjects_filter=subject)
        return info.state.messages

    @asynccontextmanager
    async def pull_subscribe(self, subject: str, stream_seq: int = 0):
        sub = await self.js.pull_subscribe(
            subject,
            stream=self._stream,
            config=ConsumerConfig(
                opt_start_seq=stream_seq, deliver_policy=deliver_policy_for_seq(stream_seq)
            ),
        )

        yield sub

        await sub.unsubscribe()


class StreamWriter:
    def __init__(self, nc: NATS, stream: str) -> None:
        self.js = nc.jetstream()
        self._stream = stream

    async def write(self, subject: str, data: bytes):
        await self.js.publish(subject, data, stream=self._stream)


class SerializerProtocol(Protocol):
    def serialize(self, event: EventEnvelope) -> bytes: ...
    def deserialize(self, data: bytes) -> EventEnvelope: ...


class JsonSerializer(SerializerProtocol):
    def serialize(self, event: EventEnvelope) -> bytes:
        return json.dumps(asdict(event)).encode("utf-8")

    def deserialize(self, data: bytes) -> EventEnvelope:
        payload = json.loads(data)
        return EventEnvelope(**payload)


class EventStore:
    def __init__(self, nc: NATS, stream: str, app_name: str) -> None:
        self.app_name = app_name
        self.stream = stream
        self.reader = StreamReader(nc, stream)
        self.writer = StreamWriter(nc, stream)
        self.jsm = nc.jsm()
        self.serializer: SerializerProtocol = JsonSerializer()

    async def ensure_stream(
        self,
        *,
        subjects: Iterable[str],
        retention: RetentionPolicy = RetentionPolicy.LIMITS,
        max_age: float | None = None,
        max_bytes: int | None = None,
    ) -> None:
        try:
            await self.jsm.stream_info(self.stream)
            return
        except NotFoundError:
            pass

        config = StreamConfig(
            name=self.stream,
            subjects=list(subjects),
            retention=retention,
            max_age=max_age,
            max_bytes=max_bytes,
        )
        await self.jsm.add_stream(config)

    async def current_version(self, aggregate_type: str, aggregate_id: str) -> int:
        subject = make_subject(self.app_name, aggregate_type, aggregate_id)
        try:
            info = await self.jsm.stream_info(self.stream, subjects_filter=subject)
        except NotFoundError:
            return 0
        return info.state.messages

    async def append(
        self,
        aggregate_type: str,
        aggregate_id: str,
        *,
        expected_version: int | None,
        events: List[Dict[str, Any]],
        metadata: Dict[str, Any] | None = None,
    ) -> List[EventEnvelope]:
        current_version = await self.current_version(aggregate_type, aggregate_id)
        if expected_version is not None and current_version != expected_version:
            raise ConcurrencyError(
                f"Expected version {expected_version}, got {current_version}"
            )

        stored_events: List[EventEnvelope] = []
        sequence = current_version
        subject = make_subject(self.app_name, aggregate_type, aggregate_id)
        for event in events:
            sequence += 1
            envelope = EventEnvelope(
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                name=event["name"],
                payload=event.get("payload", {}),
                sequence=sequence,
                metadata=metadata,
            )
            data = self.serializer.serialize(envelope)
            await self.writer.write(subject, data)
            stored_events.append(envelope)
        return stored_events

    async def load(
        self, aggregate_type: str, aggregate_id: str, *, from_sequence: int = 0
    ) -> List[EventEnvelope]:
        subject = make_subject(self.app_name, aggregate_type, aggregate_id)
        messages = await self.reader.read_messages(subject)
        events = [self.process_message(message) for message in messages]
        return [event for event in events if event.sequence > from_sequence]

    def process_message(self, message: Msg) -> EventEnvelope:
        return self.serializer.deserialize(message.data)
