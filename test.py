import asyncio

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost")
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
    script = redis.register_script(text)
    value = await script(['prueba'], [5])
    print(value)


if __name__ == "__main__":
    asyncio.run(main())