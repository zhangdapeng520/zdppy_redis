#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/15 8:15
# @Author  : 张大鹏
# @Site    : 
# @File    : z01_读写配置.py
# @Software: PyCharm
import json

from zdppy_redis import Redis

config = {
    "debug": True,
    "server": {
        "host": "127.0.0.1",
        "port": 8888
    }
}

config_secret = {
    "debug": False,
    "server": {
        "host": "192.168.11.16",
        "port": 8889
    }
}

# 读取配置
r = Redis(config=config, config_secret=config_secret)
r.log.info(r.config)

# 设置配置
r.set("test", json.dumps({"a": 11, "b": 22}))
r.update_config("test")
r.log.info(r.config)

# 写入配置
r.save_config()
r.log.info(json.loads(r.get("zdppy_redis_config")))
