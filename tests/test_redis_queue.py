import sys
from pathlib import Path
import pytest
import asyncio

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from matching.redis_queue import RedisQueue


@pytest.mark.asyncio
async def test_push_and_get():
    queue = RedisQueue(key_name_prefix="test")
    await queue.push({"name": "test", "age": 20})
    item = await queue.get()
    assert item == {"name": "test", "age": 20}


@pytest.mark.asyncio
async def test_get_empty():
    queue = RedisQueue(key_name_prefix="test")
    item = await queue.get()
    assert item is None
