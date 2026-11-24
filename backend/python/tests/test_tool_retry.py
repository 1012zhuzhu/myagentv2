import asyncio
import pytest

from app.schemas.message import ToolCall
from app.tools.toolrunner import ToolRunner
from app.tools.toolcache import ToolCache
from app.tools.validator import SchemaValidator
from app.tools.toolrunner import ToolRunner
from app.tools.toolrunner import ToolRunner
from app.schemas.tooldefinition import Tool, RetryPolicy
class FakeRegistry:
    def __init__(self, tool):
        self.tool = tool

    def get(self, name):
        if name != self.tool.name:
            raise ValueError("Tool not found")
        return self.tool


def create_runner(execute, max_attempts=3):
    tool = Tool(
        name="test_tool",
        description="测试工具",
        parameters={},
        execute=execute,
        cacheable=True,
        cache_ttl=60,
        retry_policy=RetryPolicy(
            max_attempts=max_attempts,
            retry_delay=0
        )
    )

    return ToolRunner(
        registry=FakeRegistry(tool),
        validator=SchemaValidator(),
        cache=ToolCache()
    )


def create_tool_call():
    # 请按你项目 ToolCall 的实际字段定义调整
    return ToolCall(
        tool_call_id="call_1",
        tool_name="test_tool",
        args={}
    )


@pytest.mark.asyncio
async def test_retry_then_success():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1

        if calls == 1:
            raise TimeoutError("timeout")

        return {"result": "success"}

    runner = create_runner(execute)
    result = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert calls == 2
    assert result["success"] is True
    assert result["result"] == {"result": "success"}


@pytest.mark.asyncio
async def test_all_attempts_failed():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1
        raise TimeoutError(f"timeout {calls}")

    runner = create_runner(execute)
    result = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert calls == 3
    assert result["success"] is False
    assert result["error"]["message"] == "timeout 3"


@pytest.mark.asyncio
async def test_non_retryable_error():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1
        raise ValueError("invalid value")

    runner = create_runner(execute)
    result = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert calls == 1
    assert result["success"] is False
    assert result["error"]["message"] == "invalid value"


@pytest.mark.asyncio
async def test_cache_hit_after_success():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1
        return {"result": "success"}

    runner = create_runner(execute)

    first = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])
    second = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert first["success"] is True
    assert second["success"] is True
    assert calls == 1


@pytest.mark.asyncio
async def test_single_attempt():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1
        raise TimeoutError("timeout")

    runner = create_runner(execute, max_attempts=1)
    result = await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert calls == 1
    assert result["success"] is False


@pytest.mark.asyncio
async def test_cancellation_is_not_retried():
    calls = 0

    async def execute(args):
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError()

    runner = create_runner(execute)

    with pytest.raises(asyncio.CancelledError):
        await runner.execute(create_tool_call(), allowed_tools=["test_tool"])

    assert calls == 1
