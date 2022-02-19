import asyncio
import aioredis
from zdppy_redis import AsyncRedis

r = AsyncRedis(host="localhost", port=6379, database=0)


async def set():
    """
    测试设置和获取
    :return:
    """
    await r.set("my-key", "value")
    value = await r.get("my-key")
    print(value)


async def hset():
    """
    测试设置和获取
    :return:
    """
    await r.hset("hash", {"key1": "value1", "key2": "value2", "key3": 123})
    value = await r.hgetall("hash")
    print(value)


if __name__ == "__main__":
    # asyncio.run(set())
    asyncio.run(hset())
