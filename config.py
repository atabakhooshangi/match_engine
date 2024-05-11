#!/usr/bin/env python
# encoding: utf-8

# product
product_id = "BTC-USD"
base_currency = "BTC"
quote_currency = "USD"
base_scale = 6
quote_scale = 2

# redis
redis_ip = "localhost"
redis_port = 6379

# kafka
kafka_brokers = ["localhost:9092"]
group_id = "order-reader-{}-group".format(product_id)
