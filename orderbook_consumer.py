#!/usr/bin/env python
# encoding: utf-8
from asyncio import wait_for, sleep, gather, run, TimeoutError
from typing import Optional
import json
import websockets
from aiokafka import ConsumerRecord

from config import redis_ip, redis_port
from utils.kafka import KafkaConsumer
from utils.redis import RedisClient

async def send_message_to_websocket(message):
    async with websockets.connect('ws://localhost:8765') as websocket:
        await websocket.send(message)




# a consumer to consume orderbook topic using kafka consumer
async def orderbook_consume():
    consumer = KafkaConsumer(["localhost:29092"], "BTC-USDT_order_book", "orderbook-dist-group")
    await consumer.start()

    while True:
        try:
            message_task = consumer.fetch_message()
            message: ConsumerRecord = await wait_for(message_task, timeout=5)
            # print(f"Consumed message {message}")
            msg: dict = json.loads(message.value.decode("utf8"))
            # match_messages.append(msg) if msg.get('type') == 'match' else None
            # await red.set("BTC-USD-match", json.dumps(messages))
            print(msg)
            data = json.dumps({"bids": msg['bids'], "asks": msg['asks']})

            await send_message_to_websocket(data)
        except TimeoutError:
            print('timeout')
            # print(match_messages)
            # if len(match_messages) >0:
            #     print(sum([float(x.get('size')) for x in match_messages]),'matched size')
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    run(orderbook_consume())
