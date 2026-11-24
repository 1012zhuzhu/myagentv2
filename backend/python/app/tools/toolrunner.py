import asyncio

from app.schemas.message import ToolCall
from app.tools.registry import ToolRegistry
from app.tools.validator import SchemaValidator
from app.tools.toolcache import CacheBackend


class ToolRunner:
    def __init__(
        self,
        registry: ToolRegistry,
        validator: SchemaValidator,
        cache: CacheBackend
    ):
        self.registry = registry
        self.validator = validator
        self.cache = cache

    # 判断异常是否值得重试
    @staticmethod
    def should_retry(error: Exception) -> bool:
        return isinstance(error, (TimeoutError, ConnectionError))



    async def execute(
        self,
        tool_call: ToolCall,
        allowed_tools: list[str]
    ):
        if tool_call.tool_name not in allowed_tools:
            return {
                "success": False,
                "result": None,
                "error": {
                    "code": "TOOL_NOT_ALLOWED",
                    "message": f"Tool not allowed: {tool_call.tool_name}"
                }
            }

        # 1. 查找工具
        try:
            tool = self.registry.get(tool_call.tool_name)
        except ValueError:
            return {
                "success": False,
                "result": None,
                "error": {
                    "code": "TOOL_NOT_FOUND",
                    "message": f"Tool not found: {tool_call.tool_name}"
                }
            }

        # 2. 校验工具参数
        args = tool_call.args

        validator_result = self.validator.validate(
            tool.parameters,
            args
        )

        if not validator_result["success"]:
            return {
                "success": False,
                "result": None,
                "error": {
                    "code": "INVALID_ARGUMENT",
                    "message": validator_result["error"]
                }
            }

        # 3. 检查缓存
        cache_key = None

        if tool.cacheable:
            try:
                cache_key = self.cache.make_key(tool.name, args)

                cache_hit, cached_result = await self.cache.get(cache_key)

                if cache_hit:
                    return {
                        "success": True,
                        "result": cached_result,
                        "error": None
                    }

            except Exception:
                # 缓存不可用，跳过缓存，继续执行工具
                cache_key = None

        # 4. 执行工具并进行有限重试
        last_error = None

        for attempt in range(tool.retry_policy.max_attempts):
            try:
                result = await tool.execute(args)

            except Exception as e:
                last_error = e

                if not self.should_retry(e):
                    break

                if attempt < tool.retry_policy.max_attempts - 1:
                    await asyncio.sleep(tool.retry_policy.retry_delay)

            else:
                # 5. 只缓存成功结果
                if tool.cacheable and cache_key is not None:
                    try:
                        await self.cache.set(
                            cache_key,
                            result,
                            tool.cache_ttl
                        )
                    except Exception:
                        # 缓存写入失败，不影响工具执行结果
                        pass

                # 注意：无论工具是否支持缓存，执行成功都要返回
                return {
                    "success": True,
                    "result": result,
                    "error": None
                }

        # 6. 所有尝试结束，返回最终错误
        return {
            "success": False,
            "result": None,
            "error": {
                "code": "TOOL_EXECUTION_ERROR",
                "message": str(last_error)
            }
        }
