import asyncio

from .streams import LimaStream, Entry
from .logger import logger

task_semaphore = asyncio.BoundedSemaphore(10)


async def run_job(cb, event, stream):
    post_events = []
    ack_events = []
    try:
        response = await cb(event)
        ack_events.append(event._entry)
        if isinstance(response, list):
            post_events += response
    except Exception as e:
        ...
    finally:
        await stream.ack(post_events=post_events, ack_events=ack_events)


async def event_listener(tasks: set, stream: LimaStream, callbacks: dict):
    try:
        async for entry in stream.entries():
            if entry:
                for callback_mapping in callbacks.get(entry.name, []):
                    ev = callback_mapping["event_model"].parse_raw(entry.payload)
                    entry.payload = None
                    ev._entry = entry
                    cb = callback_mapping["callback"]
                    await task_semaphore.acquire()
                    t = asyncio.create_task(run_job(cb, ev, stream), name=f"run_job_{entry.id}")
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                    t.add_done_callback(lambda _: task_semaphore.release())
            else:
                logger.warning("ESPERANDO")
    except asyncio.CancelledError:
        ...
    finally:
        logger.warning("Cerrando event listener")


async def event_balancer(stream: LimaStream):
    "controla las tareas sin asignar en otros workers"
    try:
        while True:
            logger.warning("Autoasignando")
            await asyncio.sleep(30)
            await stream.autoclaim()
    except asyncio.CancelledError:
        ...
    finally:
        logger.warning("Cerrando event Balancer")


async def trim_manager():
    ...
