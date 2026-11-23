import asyncio

from app.agent.nodes.tool_node import ToolNode
from app.schemas.message import AssistantMessage, ToolCall
from app.tools.registry import ToolRegistry
from app.tools.toolcache import ToolCache
from app.tools.toolrunner import ToolRunner
from app.tools.validator import SchemaValidator
from app.tools.weather_tools import (
    weather_tool,
    weather_tool_2,
    weather_tool_3,
)


async def main():
    # 1. 创建 Registry
    registry = ToolRegistry()
    registry.register(weather_tool)
    registry.register(weather_tool_2)
    registry.register(weather_tool_3)

    # 2. 创建 Runner
    cache = ToolCache(max_entries=100)

    runner = ToolRunner(
        registry=registry,
        validator=SchemaValidator(),
        cache=cache
    )

    # 3. 并发上限设置为 2
    node = ToolNode(
        runner=runner,
        max_concurrency=2
    )

    # 4. 模拟模型一次产生 3 个 ToolCall
    state = {
        "messages": [
            AssistantMessage(
                tool_calls=[
                    ToolCall(
                        tool_call_id="call_1",
                        tool_name="weather",
                        args={"city": "上海"}
                    ),
                    ToolCall(
                        tool_call_id="call_2",
                        tool_name="weather_2",
                        args={"city": "北京"}
                    ),
                    ToolCall(
                        tool_call_id="call_3",
                        tool_name="weather_3",
                        args={"city": "深圳"}
                    ),
                ]
            )
        ]
    }

    # 5. 执行 ToolNode
    result = await node.execute(state)

    print("\n=== Tool Messages ===")

    for message in result["messages"]:
        print(message)


if __name__ == "__main__":
    asyncio.run(main())