from app.tools.knowledge_tools import create_knowledge_search_tool
from app.tools.weather_tools import weather_tool

from app.tools.registry import ToolRegistry
from app.tools.toolrunner import ToolRunner
from app.tools.validator import SchemaValidator

from app.tools.rediscache import RedisToolCache
from app.tools.toolcache import ToolCache
from app.tools.resilientcache import ResilientToolCache


def create_tool_registry(retriever):
    """创建并注册可用工具。"""
    registry = ToolRegistry()

    # 1. 注册不需要额外依赖的工具
    registry.register(weather_tool)

    # 2. 创建依赖 Retriever 的知识库工具
    knowledge_search_tool = create_knowledge_search_tool(
        retriever
    )
    registry.register(knowledge_search_tool)

    return registry


def create_tool_system(redis_client, retriever):
    """创建完整的工具执行系统。"""

    registry = create_tool_registry(retriever)

    redis_cache = RedisToolCache(redis_client)
    fallback_cache = ToolCache(max_entries=1000)

    cache = ResilientToolCache(
        redis_cache=redis_cache,
        fallback_cache=fallback_cache,
        recovery_interval=5,
    )

    validator = SchemaValidator()

    runner = ToolRunner(
        registry=registry,
        validator=validator,
        cache=cache,
    )

    return registry, runner


