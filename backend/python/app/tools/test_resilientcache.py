
import asyncio

import fakeredis.aioredis
import pytest
import pytest_asyncio
from redis.exceptions import ConnectionError

from app.tools.toolcache import ToolCache
from app.tools.rediscache import RedisToolCache
from app.tools.resilientcache import ResilientToolCache


@pytest_asyncio.fixture
async def fake_redis():
    redis = fakeredis.aioredis.FakeRedis(
        decode_responses=True
    )
    yield redis
    await redis.aclose()


@pytest_asyncio.fixture
async def cache(fake_redis):
    return ResilientToolCache(
        redis_cache=RedisToolCache(fake_redis),
        fallback_cache=ToolCache(max_entries=100),
        recovery_interval=1,
    )


@pytest.mark.asyncio
async def test_normal_operation_uses_redis(cache):
    await cache.set("key", {"value": 123}, ttl=60)

    hit, value = await cache.get("key")

    assert hit is True
    assert value == {"value": 123}
    assert cache.is_fallback is False


@pytest.mark.asyncio
async def test_redis_failure_switches_to_fallback(
    cache,
    monkeypatch,
):
    async def raise_connection_error(*args, **kwargs):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(
        cache.redis_cache,
        "get",
        raise_connection_error,
    )

    hit, value = await cache.get("key")

    assert hit is False
    assert value is None
    assert cache.is_fallback is True


@pytest.mark.asyncio
async def test_fallback_cache_can_hit(
    cache,
    monkeypatch,
):
    async def raise_connection_error(*args, **kwargs):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(
        cache.redis_cache,
        "set",
        raise_connection_error,
    )

    await cache.set("key", {"value": 123}, ttl=60)

    hit, value = await cache.get("key")

    assert hit is True
    assert value == {"value": 123}
    assert cache.is_fallback is True


@pytest.mark.asyncio
async def test_recovery_clears_fallback_cache(
    cache,
    monkeypatch,
):
    original_set = cache.redis_cache.set

    async def raise_connection_error(*args, **kwargs):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(
        cache.redis_cache,
        "set",
        raise_connection_error,
    )

    # Redis 故障，写入本地缓存
    await cache.set("key", {"value": "local"}, ttl=60)

    assert cache.is_fallback is True

    # 模拟 Redis 恢复
    monkeypatch.setattr(
        cache.redis_cache,
        "set",
        original_set,
    )

    cache._last_probe -= 2

    hit, value = await cache.get("key")

    assert cache.is_fallback is False

    # Redis 中没有这个值，本地缓存也已清空
    assert hit is False
    assert value is None

    assert await cache.fallback_cache.get("key") == (
        False,
        None,
    )


@pytest.mark.asyncio
async def test_recovery_probe_is_not_repeated_too_frequently(
    cache,
    monkeypatch,
):
    async def raise_connection_error(*args, **kwargs):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(
        cache.redis_cache,
        "get",
        raise_connection_error,
    )

    await cache.get("key")

    ping_count = 0

    async def counted_ping():
        nonlocal ping_count
        ping_count += 1
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(
        cache.redis_cache.redis,
        "ping",
        counted_ping,
    )

    # 尚未到达探测间隔
    await cache.get("key")
    await cache.get("key")
    await cache.get("key")

    assert ping_count == 0
    assert cache.is_fallback is True


@pytest.mark.asyncio
async def test_concurrent_recovery_only_probes_once(
    cache,
    monkeypatch,
):
    cache._fallback_mode = True
    cache._last_probe -= 2

    original_ping = cache.redis_cache.redis.ping
    ping_count = 0

    async def counted_ping():
        nonlocal ping_count
        ping_count += 1
        await asyncio.sleep(0.05)
        return await original_ping()

    monkeypatch.setattr(
        cache.redis_cache.redis,
        "ping",
        counted_ping,
    )

    await asyncio.gather(
        *[cache.get("key") for _ in range(10)]
    )

    assert ping_count == 1
    assert cache.is_fallback is False
