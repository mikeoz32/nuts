"""
Aggregate snapshot storage.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict

from nats import NATS
from nats.js.errors import KeyNotFoundError, NotFoundError
from nats.js.kv import KeyValue


def make_key(aggregate_id: str) -> str:
    return aggregate_id


def make_bucket_name(prefix: str, aggregate_type: str) -> str:
    return f"{prefix}_{aggregate_type}"


@dataclass
class AggregateSnapshot:
    aggregate_id: str
    aggregate_type: str
    state: Dict[str, Any]
    last_sequence: int
    timestamp: str


class SnapshotStore:
    def __init__(self, nc: NATS, *, bucket_prefix: str = "nuts_snapshots") -> None:
        self.js = nc.jetstream()
        self.bucket_prefix = bucket_prefix
        self._buckets: Dict[str, KeyValue] = {}

    async def get_bucket(self, aggregate_type: str) -> KeyValue:
        if aggregate_type in self._buckets:
            return self._buckets[aggregate_type]

        bucket_name = make_bucket_name(self.bucket_prefix, aggregate_type)
        try:
            bucket = await self.js.key_value(bucket_name)
        except NotFoundError:
            bucket = await self.js.create_key_value(bucket=bucket_name)
        self._buckets[aggregate_type] = bucket
        return bucket

    def _serialize(self, snapshot: AggregateSnapshot) -> bytes:
        return json.dumps(asdict(snapshot)).encode("utf-8")

    def _deserialize(self, payload: bytes) -> AggregateSnapshot:
        data = json.loads(payload)
        return AggregateSnapshot(**data)

    async def load(self, aggregate_type: str, aggregate_id: str):
        bucket = await self.get_bucket(aggregate_type)
        try:
            entry = await bucket.get(make_key(aggregate_id))
        except KeyNotFoundError:
            return None
        return self._deserialize(entry.value)

    async def save(
        self,
        aggregate_type: str,
        aggregate_id: str,
        state: Dict[str, Any],
        last_sequence: int,
    ) -> int:
        bucket = await self.get_bucket(aggregate_type)
        snapshot = AggregateSnapshot(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            state=state,
            last_sequence=last_sequence,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        return await bucket.put(make_key(aggregate_id), self._serialize(snapshot))
