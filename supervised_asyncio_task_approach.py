import asyncio
import json
import logging

from matching.engine import Engine
from matching.kafka_log import KafkaLogStore
from matching.kafka_order import KafkaOrderReader, OrderBookDispatcher
from matching.redis_snapshot import RedisSnapshotStore
from models.models import Product

from config import *

# Initialize a global dictionary to keep track of running tasks
active_tasks = {}


async def initialize_engine(product_data):
    try:
        product = Product(_id=product_data['id'], base_currency=product_data['base_currency'],
                          quote_currency=product_data['quote_currency'], base_scale=product_data['base_scale'],
                          quote_scale=product_data['quote_scale'])

        snapshot_store = RedisSnapshotStore(product_id=product_data['id'], ip=redis_ip, port=redis_port)

        log_store = KafkaLogStore(product_id=product.id, brokers=kafka_brokers)
        await log_store.start()
        order_reader = KafkaOrderReader(product_id=product_data['id'], brokers=kafka_brokers,
                                        group_id=product_data['group_id'])
        await order_reader.start()

        orderbook_dispatcher = OrderBookDispatcher(product_id=product_data['id'], brokers=kafka_brokers)
        await orderbook_dispatcher.start()

        engine = Engine(product=product, order_reader=order_reader, log_store=log_store,
                        snapshot_store=snapshot_store,
                        orderbook_dispatcher=orderbook_dispatcher)

        await engine.initialize_snapshot()
        await engine.start()
    except Exception as e:
        logging.error(f"Error initializing engine for product {product_data['id']}: {e}")
        raise


async def run_engine_supervisor(product_data):
    while True:
        try:
            await initialize_engine(product_data)
        except Exception as e:
            logging.error(
                f"Engine for product {product_data['id']} crashed with error: {e}. Restarting in 5 seconds...")
            await asyncio.sleep(5)


async def manage_product_tasks():
    while True:
        # Fetch product configurations from Redis
        product_configs:dict = await fetch_product_configs_from_redis()

        # new_product_ids = {product['id'] for product in product_configs}

        active_product_ids = {k for k, v in product_configs.items() if v['active']}

        # Get the currently active product IDs
        current_product_ids = set(active_tasks.keys())

        # Determine products to start (newly activated)
        products_to_start = active_product_ids - current_product_ids

        # Determine products to stop (deactivated)
        products_to_stop = current_product_ids - active_product_ids

        # Start tasks for newly activated products
        for product_key, product_data in product_configs.items():
            if product_data['id'] in products_to_start:
                logging.info(f"Starting engine for product {product_data['id']}")
                task = asyncio.create_task(run_engine_supervisor(product_data))
                active_tasks[product_data['id']] = task

        # Cancel tasks for deactivated products
        for product_id in products_to_stop:
            logging.info(f"Stopping engine for product {product_id}")
            active_tasks[product_id].cancel()
            del active_tasks[product_id]

        # Sleep for a while before checking again
        await asyncio.sleep(10)


async def fetch_product_configs_from_redis():
    # Simulate fetching product configurations from Redis
    # Replace this with actual Redis fetch logic
    import redis
    r = redis.Redis(host=redis_ip, port=redis_port, decode_responses=True)
    markets = json.loads(r.get("markets"))
    return markets
    return [
        {
            "id": "BTC-USDT",
            "base_currency": "BTC",
            "quote_currency": "USDT",
            "base_scale": 8,
            "quote_scale": 2,
            "group_id": "order-reader-BTC-USDT-group",
            "active": True
        },
        {
            "id": "ETH-USDT",
            "base_currency": "ETH",
            "quote_currency": "USDT",
            "base_scale": 6,
            "quote_scale": 2,
            "group_id": "order-reader-ETH-USDT-group",
            "active": True
        }
    ]


async def main():
    # Start the product task manager
    await manage_product_tasks()


if __name__ == "__main__":
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    asyncio.run(main())
