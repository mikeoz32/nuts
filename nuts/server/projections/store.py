"""
Projection storage backed by JetStream KV.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict

from nats import NATS
from nats.js.errors import KeyNotFoundError, NotFoundError
from nats.js.kv import KeyValue


def make_key(aggregate_type: str, aggregate_id: str) -> str:
    return f"{aggregate_type}.{aggregate_id}"


def make_bucket_name(prefix: str, projection_name: str) -> str:
    return f"{prefix}_{projection_name}"


@dataclass
class ProjectionRecord:
    projection: str
    aggregate_id: str
    aggregate_type: str
    state: Dict[str, Any] | None
    last_sequence: int
    timestamp: str


class ProjectionStore:
    def __init__(self, nc: NATS, *, bucket_prefix: str = "nuts_projections") -> None:
        self.js = nc.jetstream()
        self.bucket_prefix = bucket_prefix
        self._buckets: Dict[str, KeyValue] = {}

    async def get_bucket(self, projection_name: str) -> KeyValue:
        if projection_name in self._buckets:
            return self._buckets[projection_name]

        bucket_name = make_bucket_name(self.bucket_prefix, projection_name)
        try:
            bucket = await self.js.key_value(bucket_name)
        except NotFoundError:
            bucket = await self.js.create_key_value(bucket=bucket_name)
        self._buckets[projection_name] = bucket
        return bucket

    def _serialize(self, record: ProjectionRecord) -> bytes:
        return json.dumps(asdict(record)).encode("utf-8")

    def _deserialize(self, payload: bytes) -> ProjectionRecord:
        data = json.loads(payload)
        return ProjectionRecord(**data)

    async def load(
        self, projection_name: str, aggregate_type: str, aggregate_id: str
    ) -> ProjectionRecord | None:
        bucket = await self.get_bucket(projection_name)
        try:
            entry = await bucket.get(make_key(aggregate_type, aggregate_id))
        except KeyNotFoundError:
            return None
        return self._deserialize(entry.value)

    async def save(
        self,
        projection_name: str,
        aggregate_type: str,
        aggregate_id: str,
        state: Dict[str, Any] | None,
        last_sequence: int,
    ) -> int:
        bucket = await self.get_bucket(projection_name)
        record = ProjectionRecord(
            projection=projection_name,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            state=state,
            last_sequence=last_sequence,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        return await bucket.put(
            make_key(aggregate_type, aggregate_id), self._serialize(record)
        )
