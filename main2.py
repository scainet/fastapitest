import asyncio

from redis import asyncio as aioredis
from lima import LimaApp, LimaEvent


EVENT_BUS_NAME = "bus_de_prueba"

redis = aioredis.from_url("redis://localhost")

lima_bus_1 = LimaApp(redis=redis, event_bus=EVENT_BUS_NAME, consumer_group="LeadServer")


class EventoPrueba(LimaEvent):
    id: int = 0
    name: str = "Nombre"


@lima_bus_1.on_event(event_model=EventoPrueba)
async def gestor_evento(evento):
    print(f"Ha llegado el evento con id {evento.id} y nombre {evento.name}")

asyncio.run(lima_bus_1.run())