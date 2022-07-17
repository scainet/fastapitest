import asyncio
from redis.asyncio import Redis
from typing import Type
import signal

from .streams import LimaStream
from .events import LimaEvent
from .tasks import event_listener, event_balancer
from .logger import logger

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)


class LimaApp:
    def __init__(self, redis: Redis, stream_name: str, consumer_group: str):
        self.redis = redis
        self.callbacks = {}
        self.tasks = set()
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self._stream: LimaStream | None = None
        self.should_exit = False
        logger.warning("Clase creada")

    def listen(self, event_name: str, event_model: Type[LimaEvent]):
        def decorator(func):
            if event_name not in self.callbacks:
                self.callbacks[event_name] = []
            self.callbacks[event_name].append({"callback": func, "event_model": event_model})
            return func

        return decorator

    async def _init_stream(self):
        self._stream = LimaStream(self.redis, self.stream_name, self.consumer_group)
        await self._stream.init()

    async def _launch_listener(self):
        task = asyncio.create_task(
            event_listener(
                tasks=self.tasks,
                stream=self._stream,
                callbacks=self.callbacks,
            ),
            name=f"event_listener_{self.stream_name}_{self.consumer_group}",
        )
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _launch_balancer(self):
        # Task manager for XAUTOCLAIM
        task = asyncio.create_task(event_balancer(stream=self._stream), name="event_balancer")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def run(self, handle_shutdown: bool = True):
        if handle_shutdown:
            loop = asyncio.get_event_loop()
            for sig in HANDLED_SIGNALS:
                loop.add_signal_handler(sig, self.handle_exit, sig, None)

        await self._init_stream()
        await self._launch_listener()
        # await self._launch_balancer()

        if handle_shutdown:
            while not self.should_exit:
                await asyncio.sleep(0.5)
            await self.shutdown()

    def handle_exit(self, *args):
        self.should_exit = True

    async def shutdown(self, *args):
        for task in self.tasks:
            task.cancel()
        while self.tasks:
            await asyncio.sleep(0.5)
        logger.warning("Cierro lima")
