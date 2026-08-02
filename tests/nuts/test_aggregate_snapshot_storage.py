import pytest

from nuts.server.es.snapshot import SnapshotStore

pytestmark = pytest.mark.asyncio


async def test_get_bucket(nc):
    store = SnapshotStore(nc)

    b1 = await store.get_bucket("orders")
    b2 = await store.get_bucket("orders")

    assert b1 == b2


async def test_save_snapshot(nc):
    store = SnapshotStore(nc)

    await store.save("orders", "1", {"status": "open"}, 2)
    snap = await store.load("orders", "1")
    assert snap.state == {"status": "open"}
    assert snap.last_sequence == 2
