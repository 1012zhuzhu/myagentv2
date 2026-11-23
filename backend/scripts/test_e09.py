
import os

from app.infrastructure.database import (
    get_knowledge_base,
    create_index_task,
)
from app.rag.pipeline import PipelineConfig
from app.rag.embedder import Embedder
from app.rag.vector_store import VectorStore
from app.rag.index_builder import IndexBuilder
from app.rag.index_service import IndexService


# =========================
# 1. 基础配置
# =========================

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "data" / "agent.db"

VECTOR_DB_PATH = BASE_DIR / "data" / "lancedb"

KB_ID = 1  # 修改成数据库中实际存在的知识库 ID

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def main():

    # =========================
    # 2. 初始化统一索引配置
    # =========================

    config = PipelineConfig(
        embedding_model=MODEL_NAME,
        chunk_size=80,
        overlap=20,
        normalize_embeddings=True,
    )

    print("Pipeline Hash:", config.calculate_hash())

    # =========================
    # 3. 初始化 Embedding 模型
    # =========================

    print("\nLoading embedding model...")

    embedder = Embedder(config)

    print("Embedding model loaded.")

    # =========================
    # 4. 初始化向量数据库
    # =========================

    vector_store = VectorStore(
        db_url=VECTOR_DB_PATH,
        table_name="knowledge_vectors",
    )

    print("VectorStore initialized.")

    # =========================
    # 5. 初始化 IndexBuilder
    # =========================

    index_builder = IndexBuilder(
        vector_store=vector_store,
        embedder=embedder,
        config=config,
    )

    print("IndexBuilder initialized.")

    # =========================
    # 6. 初始化 IndexService
    # =========================

    index_service = IndexService(
        db_path=DB_PATH,
        index_builder=index_builder,
        vector_store=vector_store,
    )

    print("IndexService initialized.")

    # =========================
    # 7. 获取知识库
    # =========================

    knowledge_base = get_knowledge_base(
        DB_PATH,
        KB_ID,
    )

    if knowledge_base is None:
        raise ValueError(f"Knowledge base {KB_ID} does not exist")

    # 获取当前 Active Version
    source_version_id = knowledge_base["active_version_id"]

    print("\nKnowledge Base:", knowledge_base["name"])
    print("Source Version:", source_version_id)

    # =========================
    # 8. 创建索引任务
    # =========================

    task_id = create_index_task(
        db_path=DB_PATH,
        kb_id=KB_ID,
        pipeline_config=config,
    )

    print("\nIndex task created:", task_id)

    # =========================
    # 9. 执行完整索引构建
    # =========================

    result = index_service.build_index(
        task_id=task_id,
        source_version_id=source_version_id,
    )

    print("\n========== BUILD RESULT ==========")
    print("Version ID:", result.version_id)
    print("Document Count:", result.document_count)
    print("Chunk Count:", result.chunk_count)
    print("Vector Count:", result.vector_count)
    print("==================================")


if __name__ == "__main__":
    main()
