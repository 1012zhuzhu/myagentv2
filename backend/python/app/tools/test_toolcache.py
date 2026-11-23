
import pytest

from app.tools.toolcache import ToolCache


def test_cache_hit():
    """写入缓存后，应该能够读取"""
    cache = ToolCache()

    cache.set("key1", {"result": "Redis"}, ttl=60)

    hit, value = cache.get("key1")

    assert hit is True
    assert value == {"result": "Redis"}


def test_cache_miss():
    """不存在的缓存应该返回未命中"""
    cache = ToolCache()

    hit, value = cache.get("not_exist")

    assert hit is False
    assert value is None


def test_cache_expiration(monkeypatch):
    """缓存过期后应该返回未命中"""
    cache = ToolCache()

    current_time = 100.0
    monkeypatch.setattr(
        "app.tools.toolcache.time.monotonic",
        lambda: current_time
    )

    cache.set("key1", "Redis", ttl=10)

    hit, value = cache.get("key1")
    assert hit is True
    assert value == "Redis"

    current_time = 111.0

    hit, value = cache.get("key1")

    assert hit is False
    assert value is None


def test_cache_lru_eviction():
    """容量达到上限时，淘汰最近最少使用的数据"""
    cache = ToolCache(max_entries=2)

    cache.set("A", "value_A", ttl=60)
    cache.set("B", "value_B", ttl=60)

    # 访问 A，使 A 成为最近使用的数据
    cache.get("A")

    # 新增 C，应该淘汰 B
    cache.set("C", "value_C", ttl=60)

    assert cache.get("A")[0] is True
    assert cache.get("B")[0] is False
    assert cache.get("C")[0] is True


def test_deepcopy_protection():
    """修改读取结果不应该影响缓存内部数据"""
    cache = ToolCache()

    original = {
        "documents": ["Redis", "RabbitMQ"]
    }

    cache.set("key1", original, ttl=60)

    # 修改原始对象，不应该影响缓存
    original["documents"].append("MySQL")

    hit, value = cache.get("key1")

    assert hit is True
    assert value == {
        "documents": ["Redis", "RabbitMQ"]
    }

    # 修改读取结果，也不应该影响缓存
    value["documents"].append("Kafka")

    hit, new_value = cache.get("key1")

    assert new_value == {
        "documents": ["Redis", "RabbitMQ"]
    }


def test_cache_key():
    """参数顺序不影响 Key，但工具名称不同必须生成不同 Key"""
    key1 = ToolCache.make_key(
        "knowledge_search",
        {"query": "Redis", "top_k": 5}
    )

    key2 = ToolCache.make_key(
        "knowledge_search",
        {"top_k": 5, "query": "Redis"}
    )

    key3 = ToolCache.make_key(
        "weather_search",
        {"query": "Redis", "top_k": 5}
    )

    assert key1 == key2
    assert key1 != key3
