import asyncio
from logging import getLogger
from typing import Any, Dict, List

import nats
from nats.aio.client import Client
from nats.js.api import AckPolicy, ConsumerConfig

from nuts.server.es.eventstore import EventEnvelope, EventStore, JsonSerializer
from nuts.server.feature import ServerFeature
from nuts.server.projections.store import ProjectionStore


logger = getLogger("nuts.server.projections")


class ProjectionMessageHandler:
    def __init__(
        self,
        app,
        nc: Client,
        state: Dict[str, Any],
        projection_store: ProjectionStore,
        *,
        timeout: float,
    ) -> None:
        self.app = app
        self.nc = nc
        self.state = state
        self.projection_store = projection_store
        self.timeout = timeout
        self.serializer = JsonSerializer()

    async def __call__(self, msg, routes) -> None:
        event = self.serializer.deserialize(msg.data)
        for route in routes:
            await self.apply_route(route, event)
        await msg.ack()

    async def apply_route(self, route, event: EventEnvelope) -> None:
        if route.event_name != event.name:
            return

        record = await self.projection_store.load(
            route.projection_name, event.aggregate_type, event.aggregate_id
        )
        if record and event.sequence <= record.last_sequence:
            return

        scope = {
            "type": "projection",
            "projection": route.projection_name,
            "aggregate_type": event.aggregate_type,
            "state": self.state,
        }
        request_message = {
            "type": "projection.event",
            "event": event.name,
            "payload": event.payload,
            "projection": record.state if record else None,
            "aggregate_id": event.aggregate_id,
            "sequence": event.sequence,
            "metadata": event.metadata or {},
        }
        response = await self.call_app(scope, request_message)
        if response.get("type") == "projection.event.error":
            logger.error("Projection error: %s", response.get("error"))
            return

        await self.projection_store.save(
            route.projection_name,
            event.aggregate_type,
            event.aggregate_id,
            response.get("projection"),
            event.sequence,
        )

    async def call_app(self, scope: Dict[str, Any], request_message: Dict[str, Any]):
        request_queue: asyncio.Queue = asyncio.Queue()
        response_queue: asyncio.Queue = asyncio.Queue()
        response_received = asyncio.Event()

        await request_queue.put(request_message)

        async def send(message):
            await response_queue.put(message)
            response_received.set()

        async def receive():
            return await request_queue.get()

        task = asyncio.create_task(self.app(scope, receive, send))
        try:
            async with asyncio.timeout(self.timeout):
                await response_received.wait()
        except asyncio.TimeoutError:
            task.cancel()
            raise
        response = await response_queue.get()
        task.cancel()
        return response


class ProjectionFeature(ServerFeature):
    def __init__(self, server) -> None:
        self.server = server
        self.nc: Client | None = None
        self.js = None
        self.store: ProjectionStore | None = None
        self.stream_name = "NUTS_ES"
        self.timeout = 1.0
        self.subscriptions = []
        self.requests = set()

    async def startup(self):
        if not getattr(self.server.app, "projection_routes", None):
            logger.info("No projection routes registered")
            return

        logger.info("Starting projection feature")
        self.nc = await nats.connect(self.server._nats_url)
        self.js = self.nc.jetstream()
        self.store = ProjectionStore(self.nc)

        event_store = EventStore(
            self.nc, stream=self.stream_name, app_name=self.server.lifespan.app_name
        )
        await event_store.ensure_stream(
            subjects=[f"{self.server.lifespan.app_name}.es.event.>"]
        )

        routes_by_subscription: Dict[tuple[str, str], List[Any]] = {}
        for route in self.server.app.projection_routes:
            key = (route.projection_name, route.aggregate_type)
            routes_by_subscription.setdefault(key, []).append(route)

        for (projection_name, aggregate_type), routes in routes_by_subscription.items():
            subject = f"{self.server.lifespan.app_name}.es.event.{aggregate_type}.>"
            durable = f"projection_{projection_name}_{aggregate_type}"
            config = ConsumerConfig(ack_policy=AckPolicy.EXPLICIT)

            async def handle_message(msg, routes=routes):
                handler = ProjectionMessageHandler(
                    self.server.app,
                    self.nc,
                    self.server.lifespan.state,
                    self.store,
                    timeout=self.timeout,
                )
                task = asyncio.create_task(handler(msg, routes))
                task.add_done_callback(self.requests.discard)
                self.requests.add(task)

            subscription = await self.js.subscribe(
                subject,
                durable=durable,
                stream=self.stream_name,
                config=config,
                cb=handle_message,
            )
            self.subscriptions.append(subscription)

    async def on_tick(self, counter):
        if counter % 1000 == 1:
            logger.debug("Projection inflight requests: %s", len(self.requests))

    async def shutdown(self):
        if not self.nc:
            return
        for subscription in self.subscriptions:
            await subscription.unsubscribe()
        await self.nc.drain()
        await self.nc.close()
        logger.info("Shutting down projections")
