
from app.tools.weather_tools import (
    weather_tool,
    weather_tool_2,
    weather_tool_3,
)
from app.tools.registry import ToolRegistry
from app.tools.toolcache import ToolCache
from app.tools.toolrunner import ToolRunner
from app.tools.validator import SchemaValidator


def create_tool_registry():
    """创建并注册可用工具。"""
    registry = ToolRegistry()

    registry.register(weather_tool)
    registry.register(weather_tool_2)
    registry.register(weather_tool_3)

    return registry


def create_tool_system():
    """创建完整的工具执行系统。"""
    registry = create_tool_registry()

    cache = ToolCache(max_entries=1000)
    validator = SchemaValidator()

    runner = ToolRunner(
        registry=registry,
        validator=validator,
        cache=cache,
    )

    return registry, runner
