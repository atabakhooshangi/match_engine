#!/usr/bin/env python
# encoding: utf-8

import logging
from kafka import KafkaProducer
from decimal import Decimal
import json
from random import randint

# log_level = logging.DEBUG
# logging.basicConfig(level=log_level)
# log = logging.getLogger('kafka')
# log.setLevel(log_level)


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

    # dictionary to convert object to json
    @property
    def to_dict(self):
        return {
            'id': self.id,
            'created_at': self.created_at,
            'product_id': self.product_id,
            'user_id': self.user_id,
            'client_oid': self.client_oid,
            'price': self.price,
            'size': self.size,
            'funds': self.funds,
            'type': self.type,
            'side': self.side,
            'time_in_force': self.time_in_force,
            'status': self.status}
import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9093')
buy_orders = []
sell_orders = []
pr_type = 1
print(datetime.datetime.now(),'----------------------------')
for i in range(1, 30):
    ri = randint(1, 2)
    # ri = 1 if pr_type == 2 else 2

    price = Decimal(f"{randint(3300, 3900)}")
    size = Decimal(f"{randint(1, 5)}.00")
    # size = Decimal("0.02")
    order = Order(_id=i, created_at=datetime.datetime.now().timestamp() * 1000, product_id="BTC-USDT", user_id=i,
                  client_oid="",
                  # price=Decimal(f"{randint(15, 20)}") if ri == 2 else Decimal(f"{randint(20, 22)}"),
                  price=price,
                  size=size,
                  funds=Decimal(price * size),
                  # funds=Decimal('0'),
                  _type="market",
                  side="buy" if ri == 1 else "sell", time_in_force="GTC", status="NEW")

    message = json.dumps(vars(order), default=str)

    producer.send('matching_order_BTC-USDT', message.encode("utf8"))
    if ri == 1:
        buy_orders.append(order.to_dict)
    else:
        sell_orders.append(order.to_dict)
    # pr_type = ri

producer.flush()
producer.close()
# 1714686443721237600
# 1714686568379.937

# print(len(buy_orders), 'buy_orders', len(sell_orders), 'sell_orders')
# print(buy_orders, 'buy_orders','\n==========\n', sell_orders, 'sell_orders')
#
# buy_orders.sort(key=lambda x: (-x['price'], x['created_at']))
# sell_orders.sort(key=lambda x: (x['price'], x['created_at']))


# print(list(buy_orders), '\n-------------------\n')
# print(list(sell_orders), '\n-------------------\n')


# buy_orders = [
#     {'id': 1, 'price': Decimal('20.00'), 'size': Decimal('200.00'), 'side': 'buy'},
#     {'id': 2, 'price': Decimal('19.00'), 'size': Decimal('500.00'), 'side': 'buy'},
#     {'id': 5, 'price': Decimal('18.00'), 'size': Decimal('700.00'), 'side': 'buy'},
#     {'id': 6, 'price': Decimal('17.50'), 'size': Decimal('400.00'), 'side': 'buy'},
#     {'id': 8, 'price': Decimal('17.00'), 'size': Decimal('600.00'), 'side': 'buy'}
# ]
#
# sell_orders = [
#     {'id': 3, 'price': Decimal('17.00'), 'size': Decimal('500.00'), 'side': 'sell'},
#     {'id': 4, 'price': Decimal('17.50'), 'size': Decimal('200.00'), 'side': 'sell'},
#     {'id': 7, 'price': Decimal('18.00'), 'size': Decimal('300.00'), 'side': 'sell'},
#     {'id': 9, 'price': Decimal('19.50'), 'size': Decimal('800.00'), 'side': 'sell'},
#     {'id': 10, 'price': Decimal('20.00'), 'size': Decimal('1000.00'), 'side': 'sell'}
# ]


# def match_orders(buy_orders, sell_orders):
#     # calculate the sum of sizes for buy and sell orders
#     start_buy_size = sum([x.get('size') for x in buy_orders])
#     print(start_buy_size, 'start_buy_size')
#     start_sell_size = sum([x.get('size') for x in sell_orders])
#     print(start_sell_size, 'start_sell_size')
#
#     while True:
#         if not buy_orders or not sell_orders:
#             break
#
#         buy_order = buy_orders[0]
#         sell_order = sell_orders[0]
#
#         if buy_order.get('price') >= sell_order.get('price'):
#             print(f"Matched {buy_order.get('id')} with {sell_order.get('id')}")
#             buy_size = buy_order.get('size')
#             sell_size = sell_order.get('size')
#
#             if buy_size == sell_size:
#                 buy_orders.pop(0)
#                 sell_orders.pop(0)
#             elif buy_size > sell_size:
#                 buy_order['size'] = buy_size - sell_size
#                 sell_orders.pop(0)
#             else:
#                 sell_order['size'] = sell_size - buy_size
#                 buy_orders.pop(0)
#         else:
#             break
#     end_buy_size = sum([x.get('size') for x in buy_orders])
#     print(end_buy_size, 'end_buy_size')
#     end_sell_size = sum([x.get('size') for x in sell_orders])
#     print(end_sell_size, 'end_sell_size')
#     remaining_buy_size = start_buy_size - end_buy_size
#     remaining_sell_size = start_sell_size - end_sell_size
#     return buy_orders, sell_orders, remaining_buy_size, remaining_sell_size
#
#
# s = match_orders(buy_orders, sell_orders)
# print(s[0], len(s[0]), '\n-------------------\n')
# print(s[1], len(s[1]), '\n-------------------\n')
# print(s[2], '\n-------------------\n')
# print(s[3], '\n-------------------\n')
