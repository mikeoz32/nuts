"""
Lifespan specification
scope['type'] = "lifespan"

'lifespan.startup'- receive event
Sent to application before it server starts serving

'lifespan.startup.complete' - send event
Sent by application to server, server must wait for this event before it starts receiving requests.


'lifespan.shutdown' - receive event
Sent to the application after server has stopped consuming new requests and closed all active consumers

'lifespan.shutdown.complete' - send event
Sent by the application when it has completed cleanup. A server must wait for this message before terminating

"""

import asyncio
from logging import getLogger

logger = getLogger("nuts.server.lifespan")


class LifespanManager:
    def __init__(self, app) -> None:
        self.app = app
        self.state = dict()
        self.app_info = dict()
        self.receive_queue = asyncio.Queue()
        self.startup_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()

    @property
    def app_name(self):
        return self.app_info.get("name")

    async def startup(self):
        logger.info("Waiting service to startup.")

        loop = asyncio.get_event_loop()
        main_task = loop.create_task(self.main())  # noqa

        startup_event = {"type": "lifespan.startup"}
        await self.receive_queue.put(startup_event)
        await self.startup_event.wait()

        logger.debug(f"State: {self.state}")
        logger.debug(f"App Info: {self.app_info}")
        logger.info(f"Loaded appplication [{self.app_info.get('name')}]")
        logger.info("Service startup complete.")

    async def shutdown(self):
        logger.info("Waiting service to shutdown")

        shutdown_event = {"type": "lifespan.shutdown"}
        await self.receive_queue.put(shutdown_event)
        await self.shutdown_event.wait()

        logger.info("Service shutdown complete.")

    async def main(self):
        try:
            app = self.app

            scope = {
                "type": "lifespan",
                "nsgi": {"version": "0.1-draft"},
                "state": self.state,
                "app_info": self.app_info,
            }

            await app(scope, self.receive, self.send)
        except BaseException as ex:
            logger.error(ex)

        finally:
            self.startup_event.set()

    async def send(self, message):
        assert message["type"] in (
            "lifespan.startup.complete",
            "lifespan.shutdown.complete",
        )

        if message["type"] == "lifespan.startup.complete":
            assert not self.startup_event.is_set()
            self.startup_event.set()

        if message["type"] == "lifespan.shutdown.complete":
            assert not self.shutdown_event.is_set()
            self.shutdown_event.set()

        if message['type'] == "lifespan.startup.failed":
            self.startup_event.set()
            self.shutdown_event.set()

    async def receive(self):
        return await self.receive_queue.get()
