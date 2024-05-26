#!/usr/bin/env python
# encoding: utf-8
from asyncio import wait_for, sleep, gather, run, TimeoutError
from typing import Optional
import json

from aiokafka import ConsumerRecord

from config import redis_ip, redis_port
from utils.kafka import KafkaConsumer
from utils.redis import RedisClient


# a consumer to consume matching_message__BTC-USD topic using kafka consumer
async def consume():
    red = RedisClient(ip="localhost", port=6380)
    consumer = KafkaConsumer(["localhost:9093"], "matching_message_BTC-USD", "match-log-group")
    await consumer.start()
    match_messages = []

    while True:
        try:
            message_task = consumer.fetch_message()
            message: ConsumerRecord = await wait_for(message_task, timeout=5)
            await consumer.seek()
            # print(f"Consumed message {message}")
            msg: dict = json.loads(message.value.decode("utf8"))
            # match_messages.append(msg) if msg.get('type') == 'match' else None
            # await red.set("BTC-USD-match", json.dumps(messages))
            print(msg)
        except TimeoutError:
            print('timeout')
            pass
            # print(match_messages)
            # if len(match_messages) >0:
            #     print(sum([float(x.get('size')) for x in match_messages]),'matched size')
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    run(consume())
