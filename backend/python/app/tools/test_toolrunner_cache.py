
import pytest

from app.schemas.message import ToolCall
from app.schemas.tooldefinition import Tool, RetryPolicy
from app.tools.registry import ToolRegistry
from app.tools.validator import SchemaValidator
from app.tools.toolcache import ToolCache
from app.tools.toolrunner import ToolRunner


def create_tool(
    execute,
    cacheable=True,
    retry_policy=None,
):
    return Tool(
        name="test_tool",
        description="Test tool",
        parameters={
            "type": "object",
            "properties": {
                "city": {"type": "string"}
            },
            "required": ["city"],
            "additionalProperties": False,
        },
        execute=execute,
        cacheable=cacheable,
        cache_ttl=60,
        retry_policy=retry_policy or RetryPolicy(
            max_attempts=1,
            retry_delay=0,
        ),
    )


def create_runner(tool, cache=None):
    registry = ToolRegistry()
    registry.register(tool)

    return ToolRunner(
        registry=registry,
        validator=SchemaValidator(),
        cache=cache or ToolCache(),
    )


def create_call(city="Shanghai"):
    return ToolCall(
        tool_call_id="call_001",
        tool_name="test_tool",
        args={"city": city},
    )


@pytest.mark.asyncio
async def test_cache_hit_does_not_execute_tool_again():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return {"weather": "sunny"}

    runner = create_runner(create_tool(execute))
    tool_call = create_call()

    first = await runner.execute(tool_call, ["test_tool"])
    second = await runner.execute(tool_call, ["test_tool"])

    assert first["success"] is True
    assert second["success"] is True
    assert first["result"] == second["result"]
    assert execute_count == 1


@pytest.mark.asyncio
async def test_non_cacheable_tool_executes_every_time():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return {"result": execute_count}

    runner = create_runner(
        create_tool(execute, cacheable=False)
    )

    first = await runner.execute(
        create_call(),
        ["test_tool"],
    )
    second = await runner.execute(
        create_call(),
        ["test_tool"],
    )

    assert first["success"] is True
    assert second["success"] is True
    assert execute_count == 2


@pytest.mark.asyncio
async def test_cache_get_failure_does_not_block_tool():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return {"result": "ok"}

    class BrokenGetCache:
        @staticmethod
        def make_key(tool_name, args):
            return "test-key"

        async def get(self, key):
            raise ConnectionError("Cache unavailable")

        async def set(self, key, value, ttl):
            raise AssertionError("Should not write after get failure")

    runner = create_runner(
        create_tool(execute),
        cache=BrokenGetCache(),
    )

    result = await runner.execute(
        create_call(),
        ["test_tool"],
    )

    assert result["success"] is True
    assert result["result"] == {"result": "ok"}
    assert execute_count == 1


@pytest.mark.asyncio
async def test_cache_set_failure_does_not_block_tool():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return {"result": "ok"}

    class BrokenSetCache:
        @staticmethod
        def make_key(tool_name, args):
            return "test-key"

        async def get(self, key):
            return False, None

        async def set(self, key, value, ttl):
            raise ConnectionError("Cache unavailable")

    runner = create_runner(
        create_tool(execute),
        cache=BrokenSetCache(),
    )

    result = await runner.execute(
        create_call(),
        ["test_tool"],
    )

    assert result["success"] is True
    assert result["result"] == {"result": "ok"}
    assert execute_count == 1


@pytest.mark.asyncio
async def test_disallowed_tool_is_rejected():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return "should not execute"

    runner = create_runner(create_tool(execute))

    result = await runner.execute(
        create_call(),
        allowed_tools=[],
    )

    assert result["success"] is False
    assert result["error"]["code"] == "TOOL_NOT_ALLOWED"
    assert execute_count == 0


@pytest.mark.asyncio
async def test_invalid_arguments_are_rejected():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1
        return "should not execute"

    runner = create_runner(create_tool(execute))

    invalid_call = ToolCall(
        tool_call_id="call_002",
        tool_name="test_tool",
        args={"city": 123},
    )

    result = await runner.execute(
        invalid_call,
        ["test_tool"],
    )

    assert result["success"] is False
    assert result["error"]["code"] == "INVALID_ARGUMENT"
    assert execute_count == 0


@pytest.mark.asyncio
async def test_timeout_error_retries_then_succeeds():
    execute_count = 0

    async def execute(args):
        nonlocal execute_count
        execute_count += 1

        if execute_count == 1:
            raise TimeoutError("Temporary timeout")

        return {"result": "recovered"}

    tool = create_tool(
        execute,
        retry_policy=RetryPolicy(
            max_attempts=2,
            retry_delay=0,
        ),
    )

    runner = create_runner(tool)

    result = await runner.execute(
        create_call(),
        ["test_tool"],
    )

    assert result["success"] is True
    assert result["result"] == {"result": "recovered"}
    assert execute_count == 2
