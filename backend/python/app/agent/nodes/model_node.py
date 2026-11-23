from app.agent.state import AgentState
from app.model.base import Model, TextResponse, ToolResponse
from app.schemas.message import AssistantMessage
from app.schemas.tooldefinition import ToolDefinition


class ModelNode:
    def __init__(
        self,
        model: Model,
        tools: list[ToolDefinition]
    ):
        self.model = model
        self.tools = tools

    async def execute(self, state: AgentState):
        messages = state["messages"]

        response = await self.model.invoke(messages, self.tools)

        if isinstance(response, TextResponse):
            assistant_message = AssistantMessage(
                content=response.content
            )

        elif isinstance(response, ToolResponse):
            assistant_message = AssistantMessage(
                tool_calls=response.tool_calls
            )

        else:
            raise TypeError(
                f"Unsupported model response: {type(response)}"
            )

        return {"messages": [assistant_message]}

def create_model_node(
        model: Model,
        tools: list[ToolDefinition]
    ):
        node = ModelNode(model, tools)

        async def execute(state: AgentState):
            return await node.execute(state)

        return execute