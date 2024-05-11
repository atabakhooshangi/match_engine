#!/usr/bin/env python
# encoding: utf-8
import asyncio
from typing import Optional
import json
from config import redis_ip, redis_port
from utils.redis import RedisClient

REDIS_QUEUE_POSTFIX: str = "_queue"


class RedisQueue(object):
    def __init__(self, key_name_prefix: str):
        self.key_name_prefix = key_name_prefix
        self.key_key = "".join([key_name_prefix, REDIS_QUEUE_POSTFIX])
        self.redis_client = RedisClient(ip=redis_ip, port=redis_port)

    async def push(self, item):
        if not isinstance(item, dict):
            raise TypeError("item must be a dictionary")
        item_json = json.dumps(item)
        await self.redis_client.redis.rpush(self.key_key, item_json)

    async def get(self) -> Optional[dict]:
        item_json = await self.redis_client.redis.lpop(self.key_key)
        if item_json is not None:
            item_dict = json.loads(item_json.decode())
            if not isinstance(item_dict, dict):
                raise TypeError("Loaded JSON data must be a dictionary")
            return item_dict
        return None
