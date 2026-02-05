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


class CommandRequest:
    def __init__(self) -> None:
        self.command = None
        self.payload = dict()
        self.aggregate = None
        self.metadata = dict()

    async def __call__(self, scope, receive, send) -> Any:
        request = await receive()
        self.command = request.get("command")
        self.payload = request.get("payload", {})
        self.aggregate = request.get("aggregate")
        self.metadata = request.get("metadata", {})


class EventApplyRequest:
    def __init__(self) -> None:
        self.event = None
        self.payload = dict()
        self.aggregate = None

    async def __call__(self, scope, receive, send) -> Any:
        request = await receive()
        self.event = request.get("event")
        self.payload = request.get("payload", {})
        self.aggregate = request.get("aggregate")


class ProjectionEventRequest:
    def __init__(self) -> None:
        self.event = None
        self.payload = dict()
        self.projection = None
        self.aggregate_id = None
        self.sequence = None
        self.metadata = dict()

    async def __call__(self, scope, receive, send) -> Any:
        request = await receive()
        self.event = request.get("event")
        self.payload = request.get("payload", {})
        self.projection = request.get("projection")
        self.aggregate_id = request.get("aggregate_id")
        self.sequence = request.get("sequence")
        self.metadata = request.get("metadata", {})


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


class CommandRoute(BaseRoute):
    def __init__(self, aggregate_type, command_name, handler) -> None:
        self.aggregate_type = aggregate_type
        self.command_name = command_name
        self.handler = Bean(handler)

    def matches(self, scope, command_name) -> bool:
        if scope["type"] == "es" and scope["aggregate_type"] == self.aggregate_type:
            return command_name == self.command_name
        return False

    async def handle(self, scope, receive, send) -> None:
        request = CommandRequest()
        await request(scope, receive, send)
        self.handler.add_context(scope["state"])
        return await self.handler(
            request.payload, request.aggregate, metadata=request.metadata
        )


class AggregateEventRoute(BaseRoute):
    def __init__(self, aggregate_type, event_name, handler) -> None:
        self.aggregate_type = aggregate_type
        self.event_name = event_name
        self.handler = Bean(handler)

    def matches(self, scope, event_name) -> bool:
        if scope["type"] == "es" and scope["aggregate_type"] == self.aggregate_type:
            return event_name == self.event_name
        return False

    async def handle(self, scope, receive, send) -> None:
        request = EventApplyRequest()
        await request(scope, receive, send)
        self.handler.add_context(scope["state"])
        return await self.handler(request.payload, request.aggregate)


class ProjectionRoute(BaseRoute):
    def __init__(self, projection_name, aggregate_type, event_name, handler) -> None:
        self.projection_name = projection_name
        self.aggregate_type = aggregate_type
        self.event_name = event_name
        self.handler = Bean(handler)

    def matches(self, scope, event_name) -> bool:
        if (
            scope["type"] == "projection"
            and scope["projection"] == self.projection_name
            and scope["aggregate_type"] == self.aggregate_type
        ):
            return event_name == self.event_name
        return False

    async def handle(self, scope, receive, send) -> None:
        request = ProjectionEventRequest()
        await request(scope, receive, send)
        self.handler.add_context(scope["state"])
        return await self.handler(
            request.payload,
            request.projection,
            metadata=request.metadata,
            aggregate_id=request.aggregate_id,
            sequence=request.sequence,
            aggregate_type=scope["aggregate_type"],
        )


class Nuts:
    def __init__(self, name: str, *, lifespan: Lifespan = None) -> None:
        self.name = name
        self.lifespan_context = lifespan
        self.method_routes = list()
        self.command_routes = list()
        self.event_routes = list()
        self.projection_routes = list()

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

    def add_projection(
        self, projection_name: str, aggregate_type: str, event_name: str, handler
    ):
        """
        Register projection handler for an aggregate event.
        """
        self.register_projection_event(
            projection_name, aggregate_type, event_name, handler
        )

    def add_command(self, aggregate_type: str, command_name: str, handler):
        """
        Register command handler for aggregate.
        """
        self.register_command(aggregate_type, command_name, handler)

    def add_aggregate_event(self, aggregate_type: str, event_name: str, handler):
        """
        Register aggregate event handler that returns new aggregate state.
        """
        self.register_aggregate_event(aggregate_type, event_name, handler)

    def register_command(self, aggregate_type: str, command_name: str, handler):
        self.command_routes.append(
            CommandRoute(aggregate_type, command_name, handler)
        )

    def register_aggregate_event(self, aggregate_type: str, event_name: str, handler):
        self.event_routes.append(
            AggregateEventRoute(aggregate_type, event_name, handler)
        )

    def register_projection_event(
        self, projection_name: str, aggregate_type: str, event_name: str, handler
    ):
        self.projection_routes.append(
            ProjectionRoute(projection_name, aggregate_type, event_name, handler)
        )

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
        assert scope["type"] in ("rpc", "lifespan", "es", "projection")

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
            return

        if scope["type"] == "es":
            request = await receive()
            request_type = request.get("type")

            if request_type == "es.command.request":
                async def receive_request():
                    return request

                for route in self.command_routes:
                    if route.matches(scope, request.get("command")):
                        response = await route.handle(scope, receive_request, send)
                        await send(
                            {
                                "type": "es.command.response",
                                "events": response,
                            }
                        )
                        return
                await send(
                    {
                        "type": "es.command.error",
                        "error": {"code": "command_not_found", "message": "Not found"},
                    }
                )
                return

            if request_type == "es.event.request":
                async def receive_request():
                    return request

                for route in self.event_routes:
                    if route.matches(scope, request.get("event")):
                        response = await route.handle(scope, receive_request, send)
                        await send(
                            {
                                "type": "es.event.response",
                                "aggregate": response,
                            }
                        )
                        return
                await send(
                    {
                        "type": "es.event.error",
                        "error": {"code": "event_not_found", "message": "Not found"},
                    }
                )
                return

        if scope["type"] == "projection":
            request = await receive()
            request_type = request.get("type")

            if request_type == "projection.event":
                async def receive_request():
                    return request

                for route in self.projection_routes:
                    if route.matches(scope, request.get("event")):
                        response = await route.handle(scope, receive_request, send)
                        await send(
                            {
                                "type": "projection.event.response",
                                "projection": response,
                            }
                        )
                        return
                await send(
                    {
                        "type": "projection.event.error",
                        "error": {
                            "code": "projection_event_not_found",
                            "message": "Not found",
                        },
                    }
                )
                return
