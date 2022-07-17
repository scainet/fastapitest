import asyncio

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)
    text = """
local response = nil
local key = KEYS[1]
local score = ARGV[1]

local number_elements = redis.call('ZCOUNT', key, '-inf', score)
if number_elements >= 1 then
    response =  redis.call('ZPOPMIN', key)[1]
end
return response
    """
    await redis.xadd('prueba', {'hola': 'Javier', 'altura': 167})
    response = await redis.xread({'prueba': 0}, count=1)
    redis.xreadgroup()
    i = response[0][1][0][0]
    response = await redis.xread({'prueba': i}, count=1)
    print(response)


if __name__ == "__main__":
    asyncio.run(main())
