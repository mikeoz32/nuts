"""
Aggregate snapshot storage
"""

from nats import NATS
from nats.js.kv import KeyValue


def make_key(aggregate_name: str, aggregate_id: str) -> str:
    return f"{aggregate_name}.{aggregate_id}"


class AggregateSnapshot:
    def __init__(self) -> None:
        pass


class AggregateSnapshotStorage:
    def __init__(self, nc: NATS) -> None:
        self.js = nc.jetstream()
        self._bucket = None

    async def get_bucket(self) -> KeyValue:
        if not self._bucket:
            self._bucket = await self.js.create_key_value(
                bucket="nuts_aggregate_snapshots"
            )
        return self._bucket

    async def save_snapshot(self, aggregate_name: str, aggregate_id: str, state):
        bucket = await self.get_bucket()
        await bucket.put(make_key(aggregate_name, aggregate_id), state)

    async def get_snapshot(self, aggregate_name: str, aggregate_id: str):
        bucket = await self.get_bucket()
        return await bucket.get(make_key(aggregate_name, aggregate_id))
