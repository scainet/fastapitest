import random
from typing import AsyncGenerator

from pydantic import BaseModel, Json
from redis.asyncio import Redis

from .config import Settings

settings = Settings()


class Entry(BaseModel):
    id: str = ""
    name: str = ""
    stream_key: str = ""
    consumer_group: str = ""
    consumer_name: str = ""
    payload: Json | None = None


class LimaStream:
    def __init__(self, redis: Redis, stream_name: str, consumer_group: str):
        self.redis: Redis = redis
        self.stream_name: str = stream_name
        self.consumer_group: str = consumer_group
        self.consumer_name: str = ""

    async def init(self):
        stream = await self.redis.exists(self.stream_name)
        if not stream:
            await self.redis.xgroup_create(name=self.stream_name, groupname=self.consumer_group, id="$", mkstream=True)
        else:
            groups = await self.redis.xinfo_groups(self.stream_name)
            if self.consumer_group not in [group["name"].decode() for group in groups]:
                await self.redis.xgroup_create(name=self.stream_name, groupname=self.consumer_group, id="$")
        # TODO check para que no colisione con uno creado
        # q = await self.redis.xinfo_consumers(self.event_bus, self.service_name)
        self.consumer_name = f"{random.randrange(16 ** 6):06x}"

    async def entries(self) -> AsyncGenerator[Entry, None]:
        while True:
            # pending_events = True
            new_events = True
            # while pending_events:
            #    stream_entry = await self.redis.xreadgroup(
            #        self.consumer_group, self.consumer_name, {self.stream_name: "0"}, count=1
            #    )
            #    if stream_entry[0][1]:
            #        yield stream_entry[0][1][0][1][b'name'], stream_entry[0][1][0][1][b'json']
            #    else:
            #        pending_events = False
            while new_events:
                stream_entry = await self.redis.xreadgroup(
                    self.consumer_group, self.consumer_name, {self.stream_name: ">"}, count=1, block=1500
                )
                if stream_entry:
                    yield Entry.construct(
                        id=stream_entry[0][1][0][0],
                        name=stream_entry[0][1][0][1][b"name"].decode(),
                        stream_key=self.stream_name,
                        consumer_group=self.consumer_group,
                        consumer_name=self.consumer_name,
                        payload=stream_entry[0][1][0][1][b"json"].decode(),
                    )
                else:
                    new_events = False
            yield None

    async def ack(self, entries: list[Entry]):
        # TODO REDIS PIPELINE
        await self.redis.xack(self.stream_name, self.consumer_group, entries[0].id)

    async def autoclaim(self):
        await self.redis.xautoclaim(
            name=self.stream_name,
            groupname=self.consumer_group,
            consumername=self.consumer_name,
            min_idle_time=settings.min_idle_time_autoclaim,
            start_id=0,
            justid=True,
        )
