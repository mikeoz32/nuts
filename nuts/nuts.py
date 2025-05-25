from logging import basicConfig, getLogger
from typing import Any, AsyncContextManager, Callable, Dict, List, Mapping, TypeVar

from nuts.di import Bean

basicConfig(
    level="DEBUG",
)
logger = getLogger("nuts.service")


AppType = TypeVar("AppType")
Lifespan = Callable[[AppType], AsyncContextManager[Mapping[str, Any]]]


class BaseRoute:
    def matches(self, scope) -> bool:
        return False

    async def handle(self, scope, receive, send) -> None: ...


class MethodRequest:
    def __init__(self) -> None:
        self.args = list()
        self.kwargs = dict()

    async def __call__(self, scope, receive, send) -> Any:
        request = await receive()
        self.args = request.get("args", [])
        self.kwargs: Dict = request.get("kwargs", dict())


class MethodRoute(BaseRoute):
    def __init__(self, method_name, handler) -> None:
        self.method_name = method_name
        self.handler = Bean(handler)

    def matches(self, scope) -> bool:
        if scope["type"] == "rpc":
            logger.debug(f"Route {self.method_name}")
            return self.method_name == scope["method"]

    async def handle(self, scope, receive, send) -> None:
        request = MethodRequest()
        await request(scope, receive, send)
        self.handler.add_context(scope["state"])
        return await self.handler(*request.args, **request.kwargs)


class Nuts:
    def __init__(self, name: str, *, lifespan: Lifespan = None) -> None:
        self.name = name
        self.lifespan_context = lifespan
        self.method_routes = list()

    def add_method(self, name, handler):
        """
        Service method
        Requests require response
        """
        self.method_routes.append(MethodRoute(name, handler))

    def add_task(self):
        """
        Long running task, save its state in nats kv store
        """
        pass

    def add_event(self):
        """
        Some event handler
        """
        pass

    def add_stream(self):
        """
        Register stream processor
        """
        pass

    def add_command(self):
        """
        Registed command handler
        """

    def add_aggregate_event(self):
        """
        Register aggregate event handler that return new aggregate state
        """

    async def lifespan(self, scope, receive, send) -> Any:
        await receive()
        try:
            async with self.lifespan_context() as maybe_state:
                logger.debug(f" Maybe State {maybe_state}")
                scope["state"].update(maybe_state)
                scope["app_info"].update({"name": self.name})
                await send({"type": "lifespan.startup.complete"})
                await receive()
                await send({"type": "lifespan.shutdown.complete"})
        except BaseException as e:
            await send({"type": "lifespan.startup.failed"})
            logger.error(e)

    async def __call__(self, scope, receive, send) -> Any:
        logger.debug(f"Scope: {scope}")
        assert scope["type"] in ("rpc", "lifespan")

        if scope["type"] == "lifespan":
            await self.lifespan(scope, receive, send)
            return

        if scope["type"] == "rpc":
            for route in self.method_routes:
                if route.matches(scope):
                    logger.debug(f" Found route for {route.method_name}")
                    response = await route.handle(scope, receive, send)
                    logger.debug(f"Method respond with {response}")
                    await send(
                        {
                            "type": "rpc.reponse",
                            "body": response,
                            "headers": {"status": "200"},
                        }
                    )
                    return

            await send(
                {
                    "type": "rpc.reponse",
                    "body": "Not found",
                    "headers": {"status": "404"},
                }
            )
