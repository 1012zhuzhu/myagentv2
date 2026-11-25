
import json
import os
from typing import Any
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

from app.model.base import ModelResponse, TextResponse, ToolResponse

from app.schemas.message import (
    Message,
    SystemMessage,
    UserMessage,
    AssistantMessage,
    ToolMessage,
    ToolCall,
)
from app.schemas.tooldefinition import ToolDefinition


class DeepSeekModel:
    def __init__(
        self,
        api_key: str | None = None,
        model: str = "deepseek-flash",
    ):
        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")

        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is not configured")

        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com",
        )
        self.model = model

    # 将项目内部消息转换成 DeepSeek API 消息
    @staticmethod
    def _convert_messages(
        messages: list[Message],
    ) -> list[dict[str, Any]]:
        api_messages = []

        for message in messages:

            if isinstance(message, SystemMessage):
                api_messages.append({
                    "role": "system",
                    "content": message.content,
                })

            elif isinstance(message, UserMessage):
                api_messages.append({
                    "role": "user",
                    "content": message.content,
                })

            elif isinstance(message, AssistantMessage):
                api_message = {
                    "role": "assistant",
                    "content": message.content,
                }

                if message.tool_calls:
                    api_message["tool_calls"] = [
                        {
                            "id": call.tool_call_id,
                            "type": "function",
                            "function": {
                                "name": call.tool_name,
                                "arguments": json.dumps(
                                    call.args,
                                    ensure_ascii=False,
                                ),
                            },
                        }
                        for call in message.tool_calls
                    ]

                api_messages.append(api_message)

            elif isinstance(message, ToolMessage):
                api_messages.append({
                    "role": "tool",
                    "tool_call_id": message.tool_call_id,
                    "content": message.content,
                })

            else:
                raise TypeError(
                    f"Unsupported message type: {type(message)}"
                )
            
        return api_messages

    # 将项目内部工具定义转换成 DeepSeek Function 格式
    @staticmethod
    def _convert_tools(
        tools: list[ToolDefinition],
    ) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                },
            }
            for tool in tools
        ]

    # 调用真实模型
    async def invoke(
        self,
        messages: list[Message],
        tools: list[ToolDefinition],
    ) -> ModelResponse:

        api_messages = self._convert_messages(messages)

        request_args: dict[str, Any] = {
            "model": self.model,
            "messages": api_messages,
        }

        if tools:
            request_args["tools"] = self._convert_tools(tools)
            request_args["tool_choice"] = "auto"

        response = await self.client.chat.completions.create(
            **request_args,
        )

        if not response.choices:
            raise ValueError("DeepSeek returned no choices")

        result = response.choices[0].message

        # 模型决定调用工具
        if result.tool_calls:
            tool_calls = []

            for call in result.tool_calls:
                try:
                    args = json.loads(call.function.arguments)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid tool arguments: {call.function.arguments}"
                    ) from exc

                if not isinstance(args, dict):
                    raise ValueError(
                        "Tool arguments must be a JSON object"
                    )

                tool_calls.append(
                    ToolCall(
                        tool_call_id=call.id,
                        tool_name=call.function.name,
                        args=args,
                    )
                )

            return ToolResponse(tool_calls=tool_calls)

        # 模型直接返回文本
        return TextResponse(
            content=result.content or "",
        )
