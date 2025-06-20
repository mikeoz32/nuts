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
from dataclasses import dataclass
import json
from logging import getLogger
from typing import Any, Dict
import nats
from nats.aio.client import Client
from nats.js.client import JetStreamContext
from nuts.server.feature import ServerFeature


logger = getLogger("nuts.server.es")


@dataclass
class CommandMessage:
    aggregate_id: str
    command_class: str
    payload: Dict[str, Any]

    def create_payload(self):
        mod = __import__(self.payload_mod, fromlist=[self.payload_class_name])
        class_ = getattr(mod, self.payload_class_name)
        return class_(**self.payload)

    @property
    def payload_mod(self):
        return ".".join(self.command_class.split(".")[0:-1])

    @property
    def payload_class_name(self):
        return self.command_class.split(".")[-1]


class CommandHandler:
    def __init__(self) -> None:
        self.request_sent = asyncio.Event()
        self.request_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.response_received = asyncio.Event()

    async def __call__(self, msg) -> Any:
        print(msg)
        command = CommandMessage(**json.loads(msg.data))
        print(command.create_payload())

        aggregate_id = command.aggregate_id
        # retreive aggregate state from repository
        # self.repository.get_by_id(aggregate_id)


class MessageHandler: ...


class EsFeature(ServerFeature):
    def __init__(self, server):
        self.server = server
        self.nc = None
        self.js = None
        self.locks = None

    async def startup(self):
        logger.info("Starting EventSource feature")
        self.nc: Client = await nats.connect("tls://localhost:4222")

        async def handle_command(msg):
            print(msg)
            command = CommandMessage(**json.loads(msg.data))
            print(command.create_payload())

        await self.nc.subscribe(
            f"{self.server.lifespan.app_name}.es.command.*",
            self.server.lifespan.app_name,
            handle_command,
        )

    async def shutdown(self):
        await self.nc.drain()
        await self.nc.close()
        logger.info("Shutting down RPC")
