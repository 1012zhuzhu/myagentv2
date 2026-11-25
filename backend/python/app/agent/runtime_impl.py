
from app.agent.graph import AgentGraph
from app.agent.nodes.model_node import ModelNode
from app.agent.nodes.tool_node import ToolNode

from app.model.deepseek_model import DeepSeekModel
from app.model.mock_model import MockModel

from app.schemas.message import (
    SystemMessage,
    UserMessage,
    AssistantMessage,
)
from app.schemas.tooldefinition import ToolDefinition

from app.tools.setup import create_tool_system


class LangGraphRuntime:
    def __init__(self, redis_client):
        # 1. 初始化工具系统
        self.registry, self.runner = create_tool_system(redis_client)

        # 2. 将内部 Tool 转换为模型可见的 ToolDefinition
        self.tools = [
            ToolDefinition(
                name=tool.name,
                description=tool.description,
                parameters=tool.parameters
            )
            for tool in self.registry.tools.values()
        ]

    async def execute(self, request):
        # 3. 根据本次请求筛选模型可见的工具
        allowed_tools = request.config.allowed_tools

        # 检查请求中的工具是否都已注册
        registered_tools = {
            tool.name
            for tool in self.tools
        }
        unknown_tools = set(allowed_tools) - registered_tools

        if unknown_tools:
            raise ValueError(
                f"Unknown tools: {sorted(unknown_tools)}"
            )

        available_tools = [
            tool
            for tool in self.tools
            if tool.name in allowed_tools
        ]

        # 4. 根据前端配置选择模型
        model_name = request.config.model

        if model_name == "mock":
            model = MockModel()

        elif model_name in ("deepseek-flash", "deepseek-v4-pro"):
            model = DeepSeekModel(model=model_name)

        else:
            raise ValueError(
                f"Unsupported model: {model_name}"
            )

        # 5. 为本次请求创建节点
        model_node = ModelNode(
            model=model,
            tools=available_tools
        )

        tool_node = ToolNode(
            runner=self.runner,
            allowed_tools=allowed_tools
        )

        # 6. 为本次请求创建 AgentGraph
        graph = AgentGraph(
            model_node=model_node,
            tool_node=tool_node
        )

        # 7. 构造本次 Agent 的初始消息
        messages = [
            SystemMessage(
                content=request.config.system_prompt
            ),
            UserMessage(
                content=str(request.input)
            )
        ]

        # 8. 执行 AgentGraph
        result = await graph.invoke(messages)

        # 9. 获取最终消息
        messages = result["messages"]
        final_message = messages[-1]

        if not isinstance(final_message, AssistantMessage):
            raise ValueError(
                "Agent did not return a final assistant message"
            )

        return {
            "content": final_message.content,
            "model": model_name
        }
