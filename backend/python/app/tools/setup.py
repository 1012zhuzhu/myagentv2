
from app.tools.rediscache import RedisToolCache
from app.tools.toolcache import ToolCache
from app.tools.resilientcache import ResilientToolCache

from app.tools.weather_tools import (
    weather_tool,
)
from app.tools.registry import ToolRegistry
from app.tools.toolrunner import ToolRunner
from app.tools.validator import SchemaValidator


def create_tool_registry():
    """创建并注册可用工具。"""
    registry = ToolRegistry()

    registry.register(weather_tool)

    return registry


def create_tool_system(redis_client):
    """创建完整的工具执行系统。"""
    registry = create_tool_registry()

    # 1. 创建 Redis 缓存
    redis_cache = RedisToolCache(redis_client)

    # 2. 创建有容量限制的本地缓存
    fallback_cache = ToolCache(max_entries=1000)

    # 3. 创建缓存协调器
    cache = ResilientToolCache(
        redis_cache=redis_cache,
        fallback_cache=fallback_cache,
        recovery_interval=5,
    )

    validator = SchemaValidator()

    # 4. 将统一缓存接口注入 ToolRunner
    runner = ToolRunner(
        registry=registry,
        validator=validator,
        cache=cache,
    )

    return registry, runner
