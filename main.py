import asyncio

from redis import asyncio as aioredis
from lima import LimaApp, LimaEvent

from fastapi import FastAPI

app = FastAPI()

STREAM_NAME = "bus_de_prueba"

redis = aioredis.from_url("redis://localhost")

lima_bus_1 = LimaApp(redis=redis, stream_name=STREAM_NAME, consumer_group="LeadServer")


class EventoPrueba(LimaEvent):
    id: int = 0
    description: str = ""


@lima_bus_1.listen(event_name="TestEvent", event_model=EventoPrueba)
async def gestor_evento(evento):
    await asyncio.sleep(2)
    1/0
    print(f"Ha llegado el evento con id {evento.id}")
    return None


@app.on_event("startup")
async def startup_event():
    print("arranco")
    await lima_bus_1.run(handle_shutdown=False)


@app.on_event("shutdown")
async def shutdown_event():
    await lima_bus_1.shutdown()


@app.get("/")
async def root():
    return {"message": "Hello World"}
