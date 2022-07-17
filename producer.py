import asyncio

from redis import asyncio as aioredis
from lima import LimaEvent


class EventoPrueba(LimaEvent):
    id: int = 0
    sevilla: str = "Nombre"
    extra_info: str = "Todo lo que quieras"

EVENT_BUS_NAME = "bus_de_prueba"

redis = aioredis.from_url("redis://localhost")

async def main():
    e = EventoPrueba(id=12, name="nombre de prueba", extra_info="Hola señor")
    e1 = EventoPrueba(id=14, name="nombre de prueba 3", extra_info="adios señor")
    e2 = EventoPrueba(id=15, name="nombre de prueba 5", extra_info="Hola niña")
    e3 = EventoPrueba(id=16, name="nombre de prueba 6", extra_info="Hola muyayo")
    await redis.xadd(EVENT_BUS_NAME, {'name': 'TestEvent', 'json': e.json()})
    await redis.xadd(EVENT_BUS_NAME, {'name': 'TestEvent', 'json': e1.json()})
    await redis.xadd(EVENT_BUS_NAME, {'name': 'TestEvent', 'json': e2.json()})
    await redis.xadd(EVENT_BUS_NAME, {'name': 'TestEvent', 'json': e3.json()})

asyncio.run(main())