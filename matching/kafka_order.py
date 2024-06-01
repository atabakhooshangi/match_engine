#!/usr/bin/env python
# encoding: utf-8
from typing import List, Dict
import json
from models.models import Order
from utils.kafka import KafkaConsumer, KafkaProducer
from utils.utils import JsonEncoder

TOPIC_ORDER_PREFIX = "matching_order_"


class KafkaOrderReader(object):
    def __init__(self, product_id: str, brokers: List[str], group_id: str):
        self.order_reader = KafkaConsumer(brokers=brokers, topic="".join([TOPIC_ORDER_PREFIX, product_id]),
                                          group_id=group_id)

    async def start(self):
        await self.order_reader.start()

    def set_offset(self, offset):
        self.order_reader.set_offset(offset)

    async def fetch_order(self) -> (int, Order):
        message = await self.order_reader.fetch_message()
        if message is None:
            return 0, None

        order = Order.from_json_str(json_str=message.value)
        return message.offset, order


TOPIC_ORDER_BOOK_SUFFIX = "_order_book"


class OrderBookDispatcher(object):
    def __init__(self, product_id: str, brokers: List):
        self.topic = ''.join([product_id, TOPIC_ORDER_BOOK_SUFFIX])
        self.dispatcher = KafkaProducer(brokers)

    async def dispatch(self, orderbook: Dict):
        await self.dispatcher.send(topic=self.topic, payload=json.dumps(orderbook, cls=JsonEncoder).encode('utf-8'))
        await self.dispatcher.flush()

    async def start(self):
        await self.dispatcher.start()
