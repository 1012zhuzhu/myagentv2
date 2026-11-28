from typing import Literal, Union,Protocol
from pydantic import BaseModel

from app.schemas.message import Message, ToolCall
from app.schemas.tooldefinition import ToolDefinition

class TokenUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

class TextResponse(BaseModel):
    type: Literal["text"] = "text"
    content: str
    usage: TokenUsage | None = None

class ToolResponse(BaseModel):
    type: Literal["tool_calls"] = "tool_calls"
    tool_calls: list[ToolCall]
    usage: TokenUsage | None = None

ModelResponse = Union[TextResponse, ToolResponse]

class Model(Protocol):
    async def invoke(
        self,
        messages: list[Message],
        tools: list[ToolDefinition]
    ) -> ModelResponse:
        ...