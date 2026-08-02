import pytest

from industry_backup.organization.service import AggregateRepository

pytestmark = pytest.mark.asyncio


async def test_get_by_id(nc, event_publisher):
    repo = AggregateRepository(nc)
    assert repo is not None
