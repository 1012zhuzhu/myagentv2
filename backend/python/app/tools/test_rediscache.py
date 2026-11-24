
import asyncio
import pytest
import fakeredis
import pytest_asyncio

from app.tools.rediscache import RedisToolCache


@pytest_asyncio.fixture
async def redis_cache():
    """创建一个隔离的模拟 Redis 实例"""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cache = RedisToolCache(redis)

    yield cache

    await redis.aclose()


@pytest.mark.asyncio
async def test_cache_miss(redis_cache):
    """不存在的 Key 应该返回未命中"""
    hit, value = await redis_cache.get("not_exist")

    assert hit is False
    assert value is None


@pytest.mark.asyncio
async def test_cache_hit(redis_cache):
    """写入缓存后应该能够读取"""
    await redis_cache.set(
        "key1",
        {"result": "Redis"},
        ttl=60
    )

    hit, value = await redis_cache.get("key1")

    assert hit is True
    assert value == {"result": "Redis"}


@pytest.mark.asyncio
async def test_cache_expiration(redis_cache):
    """缓存超过 TTL 后应该失效"""
    await redis_cache.set(
        "key1",
        {"result": "Redis"},
        ttl=1
    )

    hit, value = await redis_cache.get("key1")

    assert hit is True
    assert value == {"result": "Redis"}

    await asyncio.sleep(1.1)

    hit, value = await redis_cache.get("key1")

    assert hit is False
    assert value is None


@pytest.mark.asyncio
async def test_cache_json_serialization(redis_cache):
    """字典和列表应该能够正确序列化和反序列化"""
    original = {
        "documents": ["Redis", "RabbitMQ", "MySQL"],
        "metadata": {
            "top_k": 5,
            "enabled": True
        }
    }

    await redis_cache.set("key1", original, ttl=60)

    hit, value = await redis_cache.get("key1")

    assert hit is True
    assert value == original


@pytest.mark.asyncio
async def test_cache_zero_ttl(redis_cache):
    """TTL 小于等于 0 时不应该写入缓存"""
    await redis_cache.set("key1", "Redis", ttl=0)

    hit, value = await redis_cache.get("key1")

    assert hit is False
    assert value is None


@pytest.mark.asyncio
async def test_cache_key_consistency():
    """相同参数不同顺序应该生成相同 Key"""
    key1 = RedisToolCache.make_key(
        "knowledge_search",
        {"query": "Redis", "top_k": 5}
    )

    key2 = RedisToolCache.make_key(
        "knowledge_search",
        {"top_k": 5, "query": "Redis"}
    )

    key3 = RedisToolCache.make_key(
        "weather_search",
        {"query": "Redis", "top_k": 5}
    )

    assert key1 == key2
    assert key1 != key3
