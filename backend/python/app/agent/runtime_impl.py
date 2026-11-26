
import logging

from app.agent.graph import AgentGraph
from app.agent.nodes.model_node import ModelNode
from app.agent.nodes.tool_node import ToolNode

from app.model.deepseek_model import DeepSeekModel
from app.model.mock_model import MockModel

from app.rag import retriever
from app.schemas.message import (
    SystemMessage,
    UserMessage,
    AssistantMessage,
)
from app.schemas.tooldefinition import ToolDefinition

from app.tools.setup import create_tool_system


logger = logging.getLogger(__name__)


class LangGraphRuntime:
    def __init__(self, redis_client,retriever):
        # 1. 初始化工具系统
        self.registry, self.runner = create_tool_system(redis_client= redis_client,retriever = retriever,)

        # 2. 将内部 Tool 转换为模型可见的 ToolDefinition
        self.tools = [
            ToolDefinition(
                name=tool.name,
                description=tool.description,
                parameters=tool.parameters
            )
            for tool in self.registry.tools.values()
        ]

        logger.info(
            "Agent runtime initialized registered_tools=%d",
            len(self.tools),
        )

    async def execute(self, request):
        run_id = request.run_id
        node_id = request.node_id

        stage = "validating_tools"

        logger.info(
            "Agent runtime execution started run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        try:
            # 3. 根据本次请求筛选模型可见的工具
            allowed_tools = request.config.allowed_tools

            logger.info(
                "validating allowed tools run_id=%s node_id=%s tool_count=%d",
                run_id,
                node_id,
                len(allowed_tools),
            )

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

            logger.info(
                "allowed tools validated run_id=%s node_id=%s available_tools=%d",
                run_id,
                node_id,
                len(available_tools),
            )

            # 4. 初始化模型
            stage = "initializing_model"

            model_name = request.config.model

            logger.info(
                "initializing model run_id=%s node_id=%s model=%s",
                run_id,
                node_id,
                model_name,
            )

            if model_name == "mock":
                model = MockModel()

            elif model_name in ("deepseek-flash", "deepseek-v4-pro"):
                model = DeepSeekModel(model=model_name)

            else:
                raise ValueError(
                    f"Unsupported model: {model_name}"
                )

            logger.info(
                "model initialized run_id=%s node_id=%s model=%s",
                run_id,
                node_id,
                model_name,
            )

            # 5. 创建 Agent 节点和 Graph
            stage = "creating_graph"

            model_node = ModelNode(
                model=model,
                tools=available_tools
            )

            tool_node = ToolNode(
                runner=self.runner,
                allowed_tools=allowed_tools
            )

            graph = AgentGraph(
                model_node=model_node,
                tool_node=tool_node
            )

            logger.info(
                "Agent graph created run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            # 6. 构造初始消息
            messages = [
                SystemMessage(
                    content=request.config.system_prompt
                ),
                UserMessage(
                    content=str(request.input)
                )
            ]

            # 7. 执行 AgentGraph
            stage = "executing_graph"

            logger.info(
                "Agent graph execution started run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            result = await graph.invoke(messages)

            logger.info(
                "Agent graph execution completed run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            # 8. 验证最终结果
            stage = "validating_result"

            messages = result["messages"]
            final_message = messages[-1]

            if not isinstance(final_message, AssistantMessage):
                raise ValueError(
                    "Agent did not return a final assistant message"
                )

            logger.info(
                "final assistant message validated run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            return {
                "content": final_message.content,
                "model": model_name
            }

        except Exception:
            logger.exception(
                "Agent runtime execution failed "
                "run_id=%s node_id=%s stage=%s",
                run_id,
                node_id,
                stage,
            )
            raise
