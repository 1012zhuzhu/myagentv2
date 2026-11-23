import asyncio
import time

from app.schemas.message import ToolCall
from app.tools.toolcache import ToolCache
from app.tools.registry import ToolRegistry
from app.tools.toolrunner import ToolRunner
from app.tools.validator import SchemaValidator
from app.tools.weather_tools import weather_tool


async def main():
    # 1. 创建 Tool 系统
    registry = ToolRegistry()
    registry.register(weather_tool)

    cache = ToolCache(max_entries=100)

    runner = ToolRunner(
        registry=registry,
        validator=SchemaValidator(),
        cache=cache
    )

    # 2. 创建完全相同的 ToolCall
    tool_call = ToolCall(
        tool_call_id="call_001",
        tool_name="weather",
        args={"city": "上海"}
    )

    print("===== 第一次执行 =====")

    start = time.perf_counter()

    result1 = await runner.execute(tool_call)

    elapsed1 = time.perf_counter() - start

    print("结果:", result1)
    print(f"耗时: {elapsed1:.3f}s")

    print("\n===== 第二次执行 =====")

    start = time.perf_counter()

    result2 = await runner.execute(tool_call)

    elapsed2 = time.perf_counter() - start

    print("结果:", result2)
    print(f"耗时: {elapsed2:.3f}s")


if __name__ == "__main__":
    asyncio.run(main())