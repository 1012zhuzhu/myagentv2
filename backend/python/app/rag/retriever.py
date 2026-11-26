
from app.infrastructure.database import get_active_version
from app.rag.embedder import Embedder
from app.rag.vector_store import VectorStore


class KnowledgeBaseNotReadyError(Exception):
    """知识库没有可用的 Active Version。"""


class Retriever:
    def __init__(
        self,
        db_path: str,
        embedder: Embedder,
        vector_store: VectorStore,
    ):
        self.db_path = db_path
        self.embedder = embedder
        self.vector_store = vector_store

    def search(
        self,
        query: str,
        knowledge_base_id: int,
        top_k: int = 5,
    ) -> list[dict]:
        if not query.strip():
            raise ValueError("Query cannot be empty")

        if knowledge_base_id <= 0:
            raise ValueError(
                "knowledge_base_id must be greater than 0"
            )

        if top_k <= 0:
            raise ValueError("top_k must be greater than 0")

        # 1. 获取知识库当前 Active Version
        active_version = get_active_version(
            self.db_path,
            knowledge_base_id,
        )

        if active_version is None:
            raise KnowledgeBaseNotReadyError(
                f"Knowledge base {knowledge_base_id} "
                "has no active version"
            )

        version_id = active_version["id"]

        # 2. 将问题转换为向量
        query_vector = self.embedder.embed_query(query)

        # 3. 在当前 Active Version 中检索
        return self.vector_store.search(
            query_vector=query_vector,
            version_id=version_id,
            limit=top_k,
        )
