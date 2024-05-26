#!/usr/bin/env python
# encoding: utf-8
import asyncio
import json
import logging
import sys
import time
from asyncio import Queue, wait, FIRST_COMPLETED, wait_for, gather
from decimal import Decimal
from typing import Optional, List

from matching.kafka_log import KafkaLogStore
from matching.kafka_order import KafkaOrderReader, OrderBookDispatcher
from matching.log import Log, MatchLog
from matching.order_book import OrderBookSnapshot, BookOrder, OrderBook
from models.models import OrderException, Product, Order
from models.types import OrderType, Side, TimeInForceType, OrderStatus
from utils.utils import JsonEncoder
from utils.window import Window


class Snapshot(object):
    def __init__(self, order_book_snapshot: Optional[OrderBookSnapshot], order_offset: int):
        self.order_book_snapshot: Optional[OrderBookSnapshot] = order_book_snapshot
        self.order_offset: int = order_offset

    @staticmethod
    def to_json_str(snapshot):
        return json.dumps(vars(snapshot), cls=JsonEncoder)

    @staticmethod
    def from_json_str(json_str: str):
        snapshot_dict = json.loads(json_str)
        order_offset = int(snapshot_dict.get("order_offset"))
        order_book_snapshot_dict = snapshot_dict.get("order_book_snapshot")
        if order_book_snapshot_dict is None:
            return Snapshot(order_book_snapshot=None, order_offset=order_offset)

        # parse order_book_snapshot
        product_id = order_book_snapshot_dict.get("product_id")
        orders_list = order_book_snapshot_dict.get("orders")
        trade_seq = int(order_book_snapshot_dict.get("trade_seq"))
        log_seq = int(order_book_snapshot_dict.get("log_seq"))
        order_id_window_dict = order_book_snapshot_dict.get("order_id_window")

        # parse orders
        orders = list()
        orders_list = list() if orders_list is None else orders_list
        for order_dict in orders_list:
            order_id = int(order_dict.get("order_id"))
            user_id = int(order_dict.get("user_id"))
            price = Decimal(order_dict.get("price"))
            size = Decimal(order_dict.get("size"))
            funds = Decimal(order_dict.get("funds"))

            _type = order_dict.get("type")
            if _type == "limit":
                order_type = OrderType.OrderTypeLimit
            elif _type == "market":
                order_type = OrderType.OrderTypeMarket
            else:
                raise OrderException("invalid OrderType")

            side = order_dict.get("side")
            if side == "buy":
                order_side = Side.SideBuy
            elif side == "sell":
                order_side = Side.SideSell
            else:
                raise OrderException("invalid Side")

            time_in_force = order_dict.get("time_in_force")
            if time_in_force == "GTC":
                order_time_in_force = TimeInForceType.GoodTillCanceled
            elif time_in_force == "IOC":
                order_time_in_force = TimeInForceType.ImmediateOrCancel
            elif time_in_force == "GTX":
                order_time_in_force = TimeInForceType.GoodTillCrossing
            elif time_in_force == "FOK":
                order_time_in_force = TimeInForceType.FillOrKill
            else:
                raise OrderException("invalid TimeInForceType")

            book_order = BookOrder(order_id=order_id, user_id=user_id, price=price,
                                   size=size, funds=funds, side=order_side,
                                   _type=order_type, time_in_force=order_time_in_force)
            orders.append(book_order)

        # parse order_id_window
        window_min = int(order_id_window_dict.get("min"))
        window_max = int(order_id_window_dict.get("max"))
        window_cap = int(order_id_window_dict.get("cap"))
        window_bitmap_data = order_id_window_dict.get("bit_map")
        order_id_window = Window.from_raw(_min=window_min, _max=window_max,
                                          _cap=window_cap, bitmap_data=window_bitmap_data)

        order_book_snapshot = OrderBookSnapshot(product_id=product_id, orders=orders,
                                                trade_seq=trade_seq, log_seq=log_seq,
                                                order_id_window=order_id_window)
        return Snapshot(order_book_snapshot=order_book_snapshot, order_offset=order_offset)


class OffsetOrder(object):
    def __init__(self, offset: int, order: Order):
        self.offset: int = offset
        self.order: Order = order


class Engine(object):
    def __init__(self, product: Product, order_reader: KafkaOrderReader, log_store: KafkaLogStore,
                 snapshot_store,
                 orderbook_dispatcher: OrderBookDispatcher
                 ):
        # productId is the unique identifier of an engine, and each product corresponds to an engine
        self.product_id: str = product.id
        # The orderBook held by the engine, corresponding to the product, needs
        # a snapshot and is restored from the snapshot
        self.order_book: OrderBook = OrderBook(product, 0, 0)
        # for reading order
        self.order_reader: KafkaOrderReader = order_reader
        # Read the starting offset of the order, which will be restored from the snapshot when it is first started
        self.order_offset: int = 0
        # Used to save orderBook log
        self.log_store: KafkaLogStore = log_store
        # The storage method of persistent snapshot should support multiple methods, such as local disk, redis, etc.
        self.snapshot_store = snapshot_store
        # The read order will be written to chan, and the offset of the order needs
        # to be carried when writing the order.
        self.order_chan: Queue = Queue(maxsize=10000)
        # The log is written to the queue, and all the logs to be written need to enter the chan and wait
        self.log_chan: Queue = Queue(maxsize=10000)
        # To initiate a snapshot request, you need to carry the offset of the last snapshot
        self.snapshot_req_chan: Queue = Queue(maxsize=32)
        # The snapshot is completely ready, you need to ensure that all data before the snapshot has been committed
        self.snapshot_approve_req_chan: Queue = Queue(maxsize=32)
        # The snapshot data is ready and all data before the snapshot has been committed
        self.snapshot_chan: Queue = Queue(maxsize=32)
        # The Order Book will be dispatched using producer for realtime order book update
        self.orderbook_dispatcher: OrderBookDispatcher = orderbook_dispatcher
        self.order_book_list: Queue = Queue(maxsize=200)

        # Pending snapshot brought from method level to class level
        self.pending_snapshot: Optional[Snapshot] = None

    def restore(self, snapshot: Snapshot):
        self.order_offset = snapshot.order_offset
        self.order_book.restore(snapshot=snapshot.order_book_snapshot)

    async def initialize_snapshot(self):
        # Get the latest snapshot and use the snapshot for recovery
        snapshot = await self.snapshot_store.get_latest()
        if snapshot is not None:
            self.restore(snapshot=snapshot)

    async def start(self):
        task1 = asyncio.create_task(self.run_fetcher())
        task2 = asyncio.create_task(self.run_order_applier())
        task3 = asyncio.create_task(self.run_snapshot_applier())
        task4 = asyncio.create_task(self.run_log_committer())
        task5 = asyncio.create_task(self.run_snapshot_committer())
        task6 = asyncio.create_task(self.run_snapshots())
        task7 = asyncio.create_task(self.dispatch_orderbook())
        await gather(task1,
                     task2,
                     task3,
                     task4,
                     task5,
                     task6,
                     task7
                     )

    # Responsible for continuously pulling orders and writing to chan
    async def run_fetcher(self):
        offset = self.order_offset
        if offset > 0:
            offset += 1

        try:
            self.order_reader.set_offset(offset)
        except Exception as ex:
            logging.fatal("set order reader offset error: {}".format(ex))
            sys.exit()

        while True:
            try:
                # print('offset before', offset)
                offset, order = await self.order_reader.fetch_order()

                await self.order_chan.put(OffsetOrder(offset=offset, order=order))
                logging.info("fetch_order: {}".format(Order.to_json_str(order)))
            except Exception as ex:
                logging.error("{}".format(str(ex)))

    # Get the order from the local queue, execute the orderBook operation, and
    # respond to the snapshot request at the same time
    async def run_order_applier(self):
        order_offset: int = 0
        await self.snapshot_store.redis_client.set("order_offset", order_offset)

        while True:
            order_task = self.order_chan.get()

            try:
                offset_order: OffsetOrder = await wait_for(order_task, timeout=5)
                logs: List[Log] = list()
                order_book = dict()
                print(offset_order.order.status, 'status----------')
                if offset_order.order.status == OrderStatus.OrderStatusCancelling:
                    logs, order_book = self.order_book.cancel_order(offset_order.order)
                else:
                    # IOC
                    if offset_order.order.time_in_force == TimeInForceType.ImmediateOrCancel:
                        logs, order_book = self.order_book.apply_order(offset_order.order)
                        # cancel the rest size
                        ioc_logs, order_book = self.order_book.cancel_order(offset_order.order)
                        if len(ioc_logs) != 0:
                            logs.extend(ioc_logs)
                    elif offset_order.order.time_in_force == TimeInForceType.GoodTillCrossing:
                        # GTX
                        if self.order_book.is_order_will_not_match(offset_order.order):
                            logs, order_book = self.order_book.apply_order(offset_order.order)
                        else:
                            logs, order_book = self.order_book.nullify_order(offset_order.order)
                    elif offset_order.order.time_in_force == TimeInForceType.FillOrKill:
                        # FOK
                        if self.order_book.is_order_will_full_match(offset_order.order):
                            logs, order_book = self.order_book.apply_order(offset_order.order)
                        else:
                            logs, order_book = self.order_book.nullify_order(offset_order.order)
                    elif offset_order.order.time_in_force == TimeInForceType.GoodTillCanceled:
                        # GTC
                        logs, order_book = self.order_book.apply_order(offset_order.order)

                # Write the log generated by orderBook to chan for persistence
                for log in logs:
                    await self.log_chan.put(log)
                await self.order_book_list.put(order_book)
                # The offset of the record order is used to determine whether a snapshot needs to be taken
                order_offset = offset_order.offset
                await self.snapshot_store.redis_client.set("order_offset", order_offset)
                # print('offset_order in applier', order_offset)
                order_book['last_offset'] = order_offset
                # print(order_book,'======orderbook akharie')
                await self.order_book_list.put(order_book)
            except asyncio.TimeoutError:
                # print('TimeoutError')
                pass
    async def run_snapshot_applier(self):

        while True:
            try:
                snap_task = self.snapshot_req_chan.get()
                snapshot: Snapshot = await wait_for(snap_task, timeout=7)
                order_offset = int(await self.snapshot_store.redis_client.get("order_offset"))
                # print('order_offset in snapshot applier', order_offset)
                if order_offset is None:
                    raise Exception("order_offset is None. wait for it")

                # Receive a snapshot request and determine whether it is really necessary to perform a snapshot
                delta = order_offset - snapshot.order_offset
                if delta < 50:
                    continue
                logging.info(
                    "should take snapshot: {} {}-[{}]-{}->".format(self.product_id, snapshot.order_offset, delta,
                                                                   order_offset))

                # Execute the snapshot, and write the snapshot data to the approval chan
                snapshot.order_book_snapshot = self.order_book.snapshot()
                snapshot.order_offset = order_offset
                await self.snapshot_approve_req_chan.put(snapshot)  # only place snapshot put in the queue


            except Exception as e:
                pass
                # print(e)

    # Persist the log generated by orderBook, and need to respond to snapshot approval
    async def run_log_committer(self):
        seq: int = self.order_book.log_seq
        logs: List[Log] = list()
        match_logs = []
        while True:

            try:
                task = self.log_chan.get()
                log: Log = await wait_for(task, timeout=7)
                # print('log in log_committer', log.get_seq())
                # print('seq----', seq)
                # discard duplicate log
                if log.get_seq() <= seq:
                    # logging.info("discard log seq={}".format(seq))
                    continue
                logs.append(log)
                # chan is not empty and buffer is not full, continue read.
                if self.log_chan.qsize() > 0 and len(logs) < 10:
                    continue

                try:
                    # store log, clean buffer
                    # for log in logs:
                    #     # print('log in log_committer---------------------------')
                    #     await self.log_store.log_writer.send('matching_message_BTC-USD',Log.to_json_str(log).encode("utf8"))
                    await self.log_store.store(logs)
                    logs.clear()
                except Exception as ex:
                    logging.fatal("{}".format(ex))
                    sys.exit()
                # approve pending snapshot
                pending_snapshot = self.pending_snapshot
                if pending_snapshot is not None and seq >= pending_snapshot.order_book_snapshot.log_seq:
                    await self.snapshot_chan.put(pending_snapshot)
                    self.pending_snapshot = None
            except asyncio.TimeoutError:
                pass
                # print(' ')

    async def run_snapshot_committer(self):

        while True:
            seq: int = self.order_book.log_seq
            try:
                task = self.snapshot_approve_req_chan.get()
                snapshot: Snapshot = await wait_for(task, timeout=7)
                # The written seq has reached or exceeded the snapshot seq, and the snapshot request is approved
                # print(seq, f'----- {snapshot.order_book_snapshot.log_seq}')
                # print(seq >= snapshot.order_book_snapshot.log_seq)
                if seq >= snapshot.order_book_snapshot.log_seq:
                    await self.snapshot_chan.put(snapshot)
                    pending_snapshot = None
                    continue
                pending_snapshot = self.pending_snapshot
                # There are currently unapproved snapshots, but there are new snapshot requests, discard the old ones
                if pending_snapshot is not None:
                    logging.info("discard snapshot request (seq={}), new one (seq={}) received".format(
                        pending_snapshot.order_book_snapshot.log_seq, snapshot.order_book_snapshot.log_seq))
                self.pending_snapshot = snapshot
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                pass

    # Initiate snapshot requests regularly, and be responsible for persisting approved snapshots
    async def run_snapshots(self):
        order_offset = self.order_offset
        # print('order_offset in run snapshots self.orderoffset', order_offset)

        while True:
            tasksn = self.snapshot_chan.get()
            try:
                # print(f"Waiting for snapshot, current offset: {order_offset}")
                snapshot: Snapshot = await wait_for(tasksn, timeout=7)
                #                 print("Snapshot received, storing...")
                await self.snapshot_store.store(snapshot=snapshot)
                logging.info("Snapshot stored: product={} OrderOffset={} LogSeq={}".format(
                    self.product_id, snapshot.order_offset, snapshot.order_book_snapshot.log_seq))
                order_offset = snapshot.order_offset
            except asyncio.TimeoutError:
                #                 print("Timeout waiting for snapshot, queue sizes before put: ReqChan={}".format(self.snapshot_req_chan.qsize()))
                # print('order_offset put in snapshot req', order_offset)
                await self.snapshot_req_chan.put(Snapshot(order_book_snapshot=None, order_offset=order_offset))
                # print("Snapshot request added, queue sizes after put: ReqChan={}".format(self.snapshot_req_chan.qsize()))

    # Additional debug to observe queue size periodically or based on conditions
    # print("Current ReqChan Queue Size: {}".format(self.snapshot_req_chan.qsize()))

    async def dispatch_orderbook(self):
        latest_dispatched_offset = 0
        print(self.orderbook_dispatcher.topic, 'topic')
        # print('in dispatch_orderbook')
        while True:
            # print('latest_dispatched_offset',latest_dispatched_offset)
            task = self.order_book_list.get()
            try:
                orderbook: dict = await wait_for(task, timeout=7)
                orderbook_offset = orderbook['last_offset']
                # print(len(orderbook['asks']), 'asks length ====')
                # print(len(orderbook['bids']), 'bids length ====')
                # print(orderbook,'=======orderbook itself')
                if orderbook_offset > latest_dispatched_offset:
                    await self.orderbook_dispatcher.dispatch(orderbook=orderbook)
                    latest_dispatched_offset = orderbook_offset
            except Exception as e:
                pass
                # print(e)

    async def test(self):
        order_offset = self.order_offset
        while True:
            if self.snapshot_chan.qsize() == 0:
                #                 print(self.snapshot_req_chan.qsize(),'====ghabl')
                await self.snapshot_req_chan.put(Snapshot(order_book_snapshot=None, order_offset=order_offset))
                #                 print(self.snapshot_req_chan.qsize(),'====bad')
                await asyncio.sleep(5)
                continue
            tasksnnn = self.snapshot_chan.get()
            this_task = asyncio.create_task(tasksnnn)
            done, pending = await wait({this_task}, return_when=FIRST_COMPLETED)
            #             print(done,'=====done!')
            for t in done:
                snapshot: Snapshot = t.result()
                #                 print("Snapshot received, storing...")
                await self.snapshot_store.store(snapshot=snapshot)
                logging.info("Snapshot stored: product={} OrderOffset={} LogSeq={}".format(
                    self.product_id, snapshot.order_offset, snapshot.order_book_snapshot.log_seq))
                order_offset = snapshot.order_offset
