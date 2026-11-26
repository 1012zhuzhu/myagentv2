
import asyncio

from app.rag.retriever import Retriever
from app.schemas.tooldefinition import Tool


def create_knowledge_search_tool(retriever: Retriever) -> Tool:

    async def search_knowledge(args: dict):
        return await asyncio.to_thread(
            retriever.search,
            query=args["query"],
            knowledge_base_id=args["knowledge_base_id"],
            top_k=args.get("top_k", 5),
        )

    knowledge_search_parameters = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "需要在知识库中检索的问题",
            },
            "knowledge_base_id": {
                "type": "integer",
                "description": "要检索的知识库 ID",
            },
            "top_k": {
                "type": "integer",
                "description": "最多返回多少条相关知识片段，默认 5",
            },
        },
        "required": ["query", "knowledge_base_id"],
        "additionalProperties": False,
    }

    return Tool(
        name="knowledge_search",
        description=(
            "在指定知识库中检索与问题相关的知识片段。"
            "当回答需要参考知识库内容时使用此工具。"
        ),
        parameters=knowledge_search_parameters,
        execute=search_knowledge,
        cacheable=False,
    )
