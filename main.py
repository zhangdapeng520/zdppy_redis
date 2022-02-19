import asyncio
import aioredis
from zdppy_redis import Redis

r = Redis(host="localhost", port=6379, database=0)


def set():
    """
    测试设置和获取
    :return:
    """
    r.set("my-key", "value")
    value = r.get("my-key")
    print(value)


def mset():
    """
    测试设置和获取
    :return:
    """
    r.mset({"a": 11, "b": 22, "c": 33})
    value = r.mget("a", "b", "c")
    print(value)


def incr():
    """
    测试设置和获取
    :return:
    """
    key = "age"
    r.set(key, 22)
    print(r.get(key))
    r.incr(key)
    print(r.get(key))


def incrbyfloat():
    """
    测试设置和获取
    :return:
    """
    key = "age"
    r.set(key, 22)
    print(r.get(key))
    r.incrbyfloat(key)
    print(r.get(key))


def hset():
    """
    测试设置和获取
    :return:
    """
    r.hset("hash", {"key1": "value1", "key2": "value2", "key3": 123})
    value = r.hgetall("hash")
    print(value)


if __name__ == "__main__":
    # set()
    # mset()
    # incr()
    incrbyfloat()
    # asyncio.run(hset())
