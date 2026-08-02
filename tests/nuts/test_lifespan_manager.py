import pytest

from nuts.server.lifespan import LifespanManager

pytestmark = pytest.mark.asyncio


async def test_lifespan_manager_startup_and_shutdown():
    async def app(scope, receive, send):
        message = await receive()
        assert message["type"] == "lifespan.startup"
        scope["state"]["ready"] = True
        scope["app_info"]["name"] = "demo"
        await send({"type": "lifespan.startup.complete"})

        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.complete"})

    manager = LifespanManager(app)

    await manager.startup()
    assert manager.state["ready"] is True
    assert manager.app_name == "demo"

    await manager.shutdown()
