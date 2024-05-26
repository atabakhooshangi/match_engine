#!/usr/bin/env python
# encoding: utf-8

# product
# product_id = "BTC-USD"
# base_currency = "BTC"
# quote_currency = "USD"
# base_scale = 6
# quote_scale = 2

# dictionary of products
products = [
    {
        "id": "BTC-USDT",
        "base_currency": "BTC",
        "quote_currency": "USDT",
        "base_scale": 6,
        "quote_scale": 2,
        "group_id": "order-reader-BTC-USDT-group"
    },
    {
        "id": "ETH-USDT",
        "base_currency": "ETH",
        "quote_currency": "USDT",
        "base_scale": 4,
        "quote_scale": 2,
        "group_id": "order-reader-ETH-USDT-group"
    }
]

# redis
redis_ip = "localhost"
redis_port = 6380

# kafka
kafka_brokers = ["localhost:29092"]
# group_id = "order-reader-{}-group".format(product_id)