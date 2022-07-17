import orjson
from pydantic import BaseModel, PrivateAttr, Json

from .streams import Entry


def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


class LimaEvent(BaseModel):
    _entry: Entry | None = PrivateAttr()

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class LimaErrorEvent(LimaEvent):
    exception: Json
