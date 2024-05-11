#!/usr/bin/env python
# encoding: utf-8

import logging
from kafka import KafkaProducer
from decimal import Decimal
import json
from random import randint

log_level = logging.DEBUG
logging.basicConfig(level=log_level)
log = logging.getLogger('kafka')
log.setLevel(log_level)


# https://github.com/CheetahExchange/orderbook-py/tree/main

class Order:
    def __init__(self, _id, created_at, product_id, user_id, client_oid, price, size, funds, _type, side, time_in_force,
                 status):
        self.id = _id
        self.created_at = created_at
        self.product_id = product_id
        self.user_id = user_id
        self.client_oid = client_oid
        self.price = price
        self.size = size
        self.funds = funds
        self.type = _type
        self.side = side
        self.time_in_force = time_in_force
        self.status = status


producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(7, 8):
    ri = randint(1, 1)
    import datetime

    order = Order(_id=i, created_at=datetime.datetime.now().timestamp() * 1000, product_id="BTC-USD", user_id=i,
                  client_oid="",
                  price=Decimal(f"{randint(15, 20)}"), size=Decimal(f"{randint(500, 1500)}.00"), funds=Decimal("0.00"),
                  _type="limit",
                  side="buy" if ri == 1 else "sell", time_in_force="GTC", status="new")

    message = json.dumps(vars(order), default=str)

    producer.send('matching_order_BTC-USD', message.encode("utf8"))

producer.flush()
producer.close()
# 1714686443721237600
# 1714686568379.937
