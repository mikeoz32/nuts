import pytest

from nuts.server.es.snapshot import AggregateSnapshotStorage

pytestmark = pytest.mark.asyncio


async def test_get_bucket(nc):
    store = AggregateSnapshotStorage(nc)

    b1 = await store.get_bucket()
    b2 = await store.get_bucket()

    assert b1 == b2


async def test_save_snapshot(nc):
    store = AggregateSnapshotStorage(nc)

    await store.save_snapshot("test", "1", b"snapshot")
    snap = await store.get_snapshot("test", "1")
    assert snap.value == b"snapshot"
