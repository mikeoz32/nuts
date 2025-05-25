import asyncio
import contextlib
import os
from types import FrameType
import nats
import signal
from nuts.server.es.feature import EsFeature
import sys
import threading
from logging import getLogger
from typing import Generator

from nuts.server.lifespan import LifespanManager
from nuts.server.rpc.feature import RpcFeature


logger = getLogger("nust.server")


HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if sys.platform == "win32":  # pragma: py-not-win32
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # Windows signal 21. Sent by Ctrl+Break.


class NsgiServer:
    def __init__(self, nats_url: str) -> None:
        self._nats_url = nats_url
        self.app = None
        self.lifespan = None
        self.nc = None
        self.started = False
        self.name = None
        self.should_exit = False
        self.force_exit = False

        self._captured_signals = []

        self.features = [RpcFeature(self), EsFeature(self)]

    def run(self, app):
        """
        Run service
        """
        self.app = app
        self.lifespan = LifespanManager(app)
        return asyncio.run(self.serve())

    async def serve(self):
        with self.capture_signals():
            await self._serve()

    async def _serve(self):
        pid = os.getpid()

        logger.info(f"Started nuts server process [{pid}]")

        await self.startup()
        await self.main_loop()
        await self.shutdown()

    async def startup(self):
        try:
            await self.lifespan.startup()
        except BaseException as e:
            logger.error(e)
            raise
        logger.info("Loading features")

        for feature in self.features:
            await feature.startup()

        self.started = True

    async def shutdown(self):
        await self.lifespan.shutdown()
        for feature in self.features:
            await feature.shutdown()

    async def main_loop(self) -> None:
        counter = 0
        should_exit = await self.on_tick(counter)
        while not should_exit:
            counter += 1
            counter = counter % 864000
            await asyncio.sleep(0.1)
            should_exit = await self.on_tick(counter)

    async def on_tick(self, counter):
        if self.should_exit or self.force_exit:
            return True
        for feature in self.features:
            await feature.on_tick(counter)
        return False

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None, None, None]:
        # Signals can only be listened to from the main thread.
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        # always use signal.signal, even if loop.add_signal_handler is available
        # this allows to restore previous signal handlers later on
        original_handlers = {
            sig: signal.signal(sig, self.handle_exit) for sig in HANDLED_SIGNALS
        }
        try:
            yield
        finally:
            for sig, handler in original_handlers.items():
                signal.signal(sig, handler)
        # If we did gracefully shut down due to a signal, try to
        # trigger the expected behaviour now; multiple signals would be
        # done LIFO, see https://stackoverflow.com/questions/48434964
        for captured_signal in reversed(self._captured_signals):
            signal.raise_signal(captured_signal)

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        self._captured_signals.append(sig)
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True  # pragma: full coverage
        else:
            self.should_exit = True
