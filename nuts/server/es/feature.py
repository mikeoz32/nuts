"""
Event Sourced feature

Provides event sourced entities as stateful services.

Server is responsible for handling commands, persising entity events and current state.

## Specification

scope['type'] = 'es'

`es.command.request` - receive message
Command to execute on aggregate

message['command'] - command name
message['command_arguments'] - dict of command arguments
message['aggregate'] - entity state, could be None
message['aggregate_id']

`es.command.response` - send message
result of command execution is one or more events that has to be persisted

message['events'] = list of events to persist

`es.event.request` - receive message
Request to apply event on aggregate. New state is expected as response, used on aggregate retreiving

message['event'] = event name
message['event_arguments'] = event data
message['aggregate'] - entity state, could be none
message['aggregate_id']


Flow:
    * Commend is received
    * Commend class is created
    * Aggregate_id is retrieved from command (id could be None)
    * Aggregate state snapshot is retreived from snapshot store or None
    * Aggregate events are retrievend from event store starting from latest sequence number of loaded snapshot
    or 0 in case there is no snapshot
    * event message is issued for every loaded event, result is stored as current aggregate state
    * Commend message is issued with command pyload and current aggregate state
    * Events retrurned by command are persisted to event store
"""

import asyncio
from dataclasses import dataclass, field
import json
from logging import getLogger
from typing import Any, Dict, List

import nats
from nats.aio.client import Client

from nuts.server.es.eventstore import ConcurrencyError, EventEnvelope, EventStore
from nuts.server.es.snapshot import SnapshotStore
from nuts.server.feature import ServerFeature


logger = getLogger("nuts.server.es")


@dataclass
class CommandMessage:
    aggregate_type: str
    aggregate_id: str
    command: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    expected_version: int | None = None
    id: str | None = None

    @classmethod
    def from_message(cls, msg) -> "CommandMessage":
        payload = json.loads(msg.data)
        return cls(**payload)


class EsMessageHandler:
    def __init__(
        self,
        app,
        nc: Client,
        state: Dict[str, Any],
        event_store: EventStore,
        snapshot_store: SnapshotStore,
        *,
        timeout: float,
        snapshot_interval: int,
    ) -> None:
        self.app = app
        self.nc = nc
        self.state = state
        self.event_store = event_store
        self.snapshot_store = snapshot_store
        self.timeout = timeout
        self.snapshot_interval = snapshot_interval

    async def __call__(self, msg) -> Any:
        try:
            command = CommandMessage.from_message(msg)
            scope = {
                "type": "es",
                "aggregate_type": command.aggregate_type,
                "aggregate_id": command.aggregate_id,
                "state": self.state,
            }

            snapshot = await self.snapshot_store.load(
                command.aggregate_type, command.aggregate_id
            )
            aggregate_state = snapshot.state if snapshot else None
            last_sequence = snapshot.last_sequence if snapshot else 0

            stored_events = await self.event_store.load(
                command.aggregate_type, command.aggregate_id, from_sequence=last_sequence
            )
            aggregate_state = await self.apply_events(
                scope, aggregate_state, stored_events
            )

            expected_version = (
                command.expected_version
                if command.expected_version is not None
                else last_sequence + len(stored_events)
            )

            command_response = await self.call_app(
                scope,
                {
                    "type": "es.command.request",
                    "command": command.command,
                    "payload": command.payload,
                    "aggregate": aggregate_state,
                    "metadata": command.metadata,
                },
            )
            if command_response.get("type") == "es.command.error":
                await self.publish_error(msg.reply, command_response.get("error", {}))
                return

            events = command_response.get("events", [])
            try:
                stored_new_events = await self.event_store.append(
                    command.aggregate_type,
                    command.aggregate_id,
                    expected_version=expected_version,
                    events=events,
                    metadata=command.metadata,
                )
            except ConcurrencyError as exc:
                await self.publish_error(
                    msg.reply,
                    {"code": "conflict_error", "message": str(exc)},
                )
                return

            aggregate_state = await self.apply_events(
                scope, aggregate_state, stored_new_events
            )
            new_version = (
                stored_new_events[-1].sequence
                if stored_new_events
                else expected_version
            )

            if (
                self.snapshot_interval
                and new_version
                and new_version % self.snapshot_interval == 0
                and aggregate_state is not None
            ):
                await self.snapshot_store.save(
                    command.aggregate_type,
                    command.aggregate_id,
                    aggregate_state,
                    new_version,
                )

            await self.publish_response(
                msg.reply,
                {
                    "type": "es.command.response",
                    "aggregate_id": command.aggregate_id,
                    "new_version": new_version,
                    "events": [self.event_to_dict(event) for event in stored_new_events],
                },
            )
        except asyncio.TimeoutError:
            await self.publish_error(
                msg.reply,
                {"code": "timeout", "message": "Command processing timed out"},
            )
        except Exception as exc:
            logger.exception("ES command handling failed")
            await self.publish_error(
                msg.reply,
                {"code": "internal_error", "message": str(exc)},
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

    async def apply_events(
        self,
        scope: Dict[str, Any],
        aggregate_state: Dict[str, Any] | None,
        events: List[EventEnvelope],
    ):
        current_state = aggregate_state
        for event in events:
            response = await self.call_app(
                scope,
                {
                    "type": "es.event.request",
                    "event": event.name,
                    "payload": event.payload,
                    "aggregate": current_state,
                },
            )
            if response.get("type") == "es.event.error":
                raise RuntimeError(response.get("error"))
            current_state = response.get("aggregate")
        return current_state

    async def publish_response(self, reply: str, payload: Dict[str, Any]):
        await self.nc.publish(reply, json.dumps(payload).encode("utf-8"))

    async def publish_error(self, reply: str, error: Dict[str, Any]):
        await self.nc.publish(
            reply, json.dumps({"type": "es.command.error", "error": error}).encode("utf-8")
        )

    def event_to_dict(self, event: EventEnvelope):
        return {
            "name": event.name,
            "payload": event.payload,
            "sequence": event.sequence,
        }


class EsFeature(ServerFeature):
    def __init__(self, server):
        self.server = server
        self.nc = None
        self.locks = None
        self.event_store = None
        self.snapshot_store = None
        self.stream_name = "NUTS_ES"
        self.snapshot_interval = 50
        self.timeout = 1.0
        self.requests = set()

    async def startup(self):
        logger.info("Starting EventSource feature")
        self.nc = await nats.connect(self.server._nats_url)
        self.event_store = EventStore(
            self.nc, stream=self.stream_name, app_name=self.server.lifespan.app_name
        )
        self.snapshot_store = SnapshotStore(self.nc)
        await self.event_store.ensure_stream(
            subjects=[f"{self.server.lifespan.app_name}.es.event.>"]
        )

        async def handle_command(msg):
            handler = EsMessageHandler(
                self.server.app,
                self.nc,
                self.server.lifespan.state,
                self.event_store,
                self.snapshot_store,
                timeout=self.timeout,
                snapshot_interval=self.snapshot_interval,
            )
            request_task = asyncio.create_task(handler(msg))
            request_task.add_done_callback(self.requests.discard)
            self.requests.add(request_task)

        await self.nc.subscribe(
            f"{self.server.lifespan.app_name}.es.command.*",
            self.server.lifespan.app_name,
            handle_command,
        )

    async def on_tick(self, counter):
        if counter % 1000 == 1:
            logger.debug("ES inflight requests: %s", len(self.requests))

    async def shutdown(self):
        await self.nc.drain()
        await self.nc.close()
        logger.info("Shutting down ES")
