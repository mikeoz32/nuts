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
"""

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


class EsFeature(ServerFeature):
    def __init__(self, server):
        self.server = server
        self.nc = None
        self.js = None
        self.locks = None

    async def startup(self):
        logger.info("Starting EventSource feature")
        self.nc: Client = await nats.connect("tls://localhost:4222")
        self.js: JetStreamContext = self.nc.jetstream()
        self.locks = await self.js.create_key_value(
            bucket=f"{self.server.lifespan.app_name}_locks", ttl=10
        )

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
