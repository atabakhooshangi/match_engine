#!/usr/bin/env python
# encoding: utf-8
import json
from decimal import Decimal, ROUND_DOWN
from enum import Enum


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        from matching.order_book import OrderBookSnapshot
        if isinstance(obj, Enum):
            return str(obj.value)
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, OrderBookSnapshot):  # handle OrderBookSnapshot objects
            return obj.to_dict()
        return super().default(obj)


def truncate_decimal(d, places) -> Decimal:
    return d.quantize(Decimal(10) ** -places, rounding=ROUND_DOWN)
