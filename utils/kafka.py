#!/usr/bin/env python
# encoding: utf-8
from random import choice
from typing import List
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class KafkaException(Exception):
    pass


class KafkaProducer(object):
    def __init__(self, brokers: List[str]):
        self.producer = AIOKafkaProducer(bootstrap_servers=','.join(brokers))

    async def start(self):
        await self.producer.start()

    async def send(self, topic: str, payload: bytes):
        await self.producer.send(topic=topic, value=payload)

    async def send_and_wait(self, topic: str, payload: bytes):
        await self.producer.send_and_wait(topic=topic, value=payload)

    async def send_batch(self, topic: str, payloads: List[bytes]):
        bat = self.producer.create_batch()
        [bat.append(key=None, value=payload, timestamp=None) for payload in payloads]
        if bat.record_count() != len(payloads):
            raise KafkaException("bat.record_count() != len(payloads)")
        partitions = await self.producer.partitions_for(topic)
        if len(partitions) == 0:
            raise KafkaException("len(partitions) == 0")
        partition = choice(list(partitions))

        await self.producer.send_batch(batch=bat, topic=topic, partition=partition)

    async def flush(self):
        await self.producer.flush()


class KafkaConsumer(object):
    def __init__(self, brokers: List[str], topic: str, group_id: str):
        self.partitions = dict()
        self.topic = topic
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(topic, bootstrap_servers=','.join(brokers),
                                         group_id=group_id)

    async def seek(self, offset=0):
        partitions = self.consumer.assignment()
        if len(partitions) == 0:
            raise KafkaException("len(self.partitions) == 0")
        for partition in partitions:
            self.consumer.seek(partition=partition, offset=offset)
    async def start(self):
        await self.consumer.start()

    def set_offset(self, offset):
        self.partitions = self.consumer.assignment()
        if len(self.partitions) == 0:
            raise KafkaException("len(self.partitions) == 0")

        for partition in self.partitions:
            self.consumer.seek(partition=partition, offset=offset)

    async def fetch_message(self):
        kafka_record = await self.consumer.getone()
        return kafka_record
