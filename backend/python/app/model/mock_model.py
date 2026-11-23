from app.model.base import ModelResponse, TextResponse, ToolResponse
from app.schemas.message import Message, ToolCall, ToolMessage
from app.schemas.tooldefinition import ToolDefinition


class MockModel:
    async def invoke(
        self,
        messages:list[Message],
        tools:list[ToolDefinition]
    ) -> ModelResponse:
        if messages and isinstance(messages[-1], ToolMessage) :
            return TextResponse(content="上海深圳杭州的大厂我都会去的")

        if not tools:
            return TextResponse(content="当前没有可用工具")
        return ToolResponse(
            tool_calls=[
                ToolCall(
                    tool_call_id="call_001",
                    tool_name=tools[0].name,
                    args={"city": "上海"}
                )
            ]
        )
        