import pytest

from nuts.server.projections.store import ProjectionStore

pytestmark = pytest.mark.asyncio


async def test_save_projection_record(nc):
    store = ProjectionStore(nc)

    await store.save("order_totals", "orders", "1", {"total": 5}, 3)
    record = await store.load("order_totals", "orders", "1")

    assert record.state == {"total": 5}
    assert record.last_sequence == 3
    assert record.aggregate_type == "orders"
