#!/usr/bin/env python
# encoding: utf-8
import asyncio
import logging

from matching.engine import Engine
from matching.kafka_log import KafkaLogStore
from matching.kafka_order import KafkaOrderReader, OrderBookDispatcher
from matching.redis_snapshot import RedisSnapshotStore
from models.models import Product

from config import *


async def main():
    product = Product(_id=products[0]['id'], base_currency=products[0]['base_currency'],
                      quote_currency=products[0]['quote_currency'], base_scale=products[0]['base_scale'],
                      quote_scale=products[0]['quote_scale'])

    snapshot_store = RedisSnapshotStore(product_id=products[0]['id'], ip=redis_ip, port=redis_port)

    log_store = KafkaLogStore(product_id=product.id, brokers=kafka_brokers)
    await log_store.start()
    order_reader = KafkaOrderReader(product_id=products[0]['id'], brokers=kafka_brokers,
                                    group_id=products[0]['group_id'])
    await order_reader.start()

    orderbook_dispatcher = OrderBookDispatcher(product_id=products[0]['id'], brokers=kafka_brokers)
    await orderbook_dispatcher.start()

    # engine
    engine = Engine(product=product, order_reader=order_reader, log_store=log_store,
                    snapshot_store=snapshot_store,
                    orderbook_dispatcher=orderbook_dispatcher
                    )

    await engine.initialize_snapshot()
    await engine.start()


if __name__ == "__main__":
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    asyncio.run(main())
