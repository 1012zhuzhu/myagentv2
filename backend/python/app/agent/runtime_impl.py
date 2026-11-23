
from app.agent.runtime import AgentRuntime
from app.agent.graph import AgentGraph
from app.agent.nodes.model_node import ModelNode
from app.agent.nodes.tool_node import ToolNode

from app.model.mock_model import MockModel

from app.schemas.message import UserMessage
from app.schemas.tooldefinition import ToolDefinition

from app.tools.setup import create_tool_system


class LangGraphRuntime:
    def __init__(self):
        # 1. 初始化工具系统
        self.registry, self.runner = create_tool_system()

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

        # 4. 为本次请求创建节点
        model_node = ModelNode(
            model=MockModel(),
            tools=available_tools
        )

        tool_node = ToolNode(
            runner=self.runner,
            allowed_tools=allowed_tools
        )

        # 5. 为本次请求创建 AgentGraph
        graph = AgentGraph(
            model_node=model_node,
            tool_node=tool_node
        )

        # 6. 将输入转换为用户消息
        user_message = UserMessage(
            content=str(request.input)
        )

        # 7. 执行 AgentGraph
        result = await graph.invoke([user_message])

        # 8. 获取最终消息
        messages = result["messages"]
        final_message = messages[-1]

        return {
            "content": final_message.content,
            "model": request.config.model
        }
