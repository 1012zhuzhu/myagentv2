
import sqlite3
from pathlib import Path

import pytest

from app.infrastructure.database import (
    init_db,
    create_knowledge_base,
    create_document,
    create_index_task,
)
from app.rag.pipeline import PipelineConfig
from app.rag.embedder import Embedder
from app.rag.index_builder import IndexBuilder
from app.rag.index_service import IndexService
from app.rag.vector_store import VectorStore
from app.rag.chunker import split_text


# 本机已经下载好的真实 Embedding 模型路径
MODEL_PATH = (
    r"C:\Users\19525\.cache\huggingface\hub"
    r"\models--sentence-transformers--paraphrase-multilingual-MiniLM-L12-v2"
    r"\snapshots\e8f8c211226b894fcb81acc59f3b34ba3efd5f42"
)


def get_version_records(vector_store, version_id):
    """
    从 LanceDB 中读取指定版本的全部向量记录。
    仅用于集成测试。
    """
    table = vector_store.db.open_table(
        vector_store.table_name
    )

    # 读取 LanceDB 表为 PyArrow Table
    arrow_table = table.to_arrow()

    # 转换为 Python 字典列表
    records = arrow_table.to_pylist()

    # 在 Python 层筛选指定版本
    return [
        record
        for record in records
        if record["version_id"] == version_id
    ]


@pytest.mark.integration
def test_real_embedding_incremental_index_build(
    tmp_path: Path,
    monkeypatch,
):
    # ==================================================
    # 1. 初始化 SQLite 测试数据库
    # ==================================================

    db_path = str(tmp_path / "test.db")

    init_db(db_path)

    kb_id = create_knowledge_base(
        db_path=db_path,
        name="real-embedding-test",
    )

    content_a = """
    Agent 是一种能够感知环境、理解用户目标并采取行动的智能系统。
    一个 Agent 通常由大语言模型、工具调用、记忆系统和任务规划组成。
    大语言模型负责理解任务和生成决策，工具负责与外部系统交互。
    RAG 通过检索知识库中的相关内容，为模型提供额外的上下文信息。
    向量数据库可以存储文本向量，并根据语义相似度检索相关内容。
    """ * 15

    document_a_id = create_document(
        db_path=db_path,
        kb_id=kb_id,
        name="agent-introduction.txt",
        content=content_a,
    )

    # ==================================================
    # 2. 配置真实 Embedding 模型
    # ==================================================

    assert Path(MODEL_PATH).exists(), (
        f"本地模型路径不存在: {MODEL_PATH}"
    )

    config = PipelineConfig(
        embedding_model=(
            "sentence-transformers/"
            "paraphrase-multilingual-MiniLM-L12-v2"
        ),
        embedding_model_path=MODEL_PATH,
        chunk_size=80,
        overlap=20,
        normalize_embeddings=True,
    )

    embedder = Embedder(config)

    # ==================================================
    # 3. 初始化 LanceDB
    # ==================================================

    vector_db_path = str(tmp_path / "lancedb")

    vector_store = VectorStore(
        db_url=vector_db_path,
    )

    # ==================================================
    # 4. 组装 IndexBuilder 和 IndexService
    # ==================================================

    index_builder = IndexBuilder(
        vector_store=vector_store,
        embedder=embedder,
        config=config,
    )

    index_service = IndexService(
        db_path=db_path,
        index_builder=index_builder,
        vector_store=vector_store,
    )

    # ==================================================
    # 5. 创建并构建 V1
    # ==================================================

    task_v1 = create_index_task(
        db_path=db_path,
        kb_id=kb_id,
        pipeline_config=config,
    )

    result_v1 = index_service.build_index(task_v1)

    version_v1 = result_v1.version_id

    assert result_v1.document_count == 1
    assert result_v1.chunk_count > 0
    assert result_v1.vector_count > 0

    # 保存 V1 的全部向量记录
    v1_records = get_version_records(
        vector_store,
        version_v1,
    )

    assert len(v1_records) == result_v1.vector_count

    # 保存 A 在 V1 中的向量，后续用于比较
    v1_a_vectors = {
        record["chunk_index"]: list(record["vector"])
        for record in v1_records
        if record["document_id"] == document_a_id
    }

    assert v1_a_vectors

    # ==================================================
    # 6. 检查 V1 的 SQLite 状态
    # ==================================================

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        task = conn.execute(
            """
            SELECT status, version_id
            FROM index_tasks
            WHERE id = ?
            """,
            (task_v1,),
        ).fetchone()

        version = conn.execute(
            """
            SELECT status, pipeline_hash
            FROM index_versions
            WHERE id = ?
            """,
            (version_v1,),
        ).fetchone()

        assert task is not None
        assert version is not None

        assert task["status"] == "ready"
        assert task["version_id"] == version_v1

        assert version["status"] == "ready"
        assert version["pipeline_hash"] == config.calculate_hash()

        snapshot = conn.execute(
            """
            SELECT document_id
            FROM version_documents
            WHERE version_id = ?
            """,
            (version_v1,),
        ).fetchall()

        snapshot_ids = {
            row["document_id"]
            for row in snapshot
        }

        assert snapshot_ids == {document_a_id}

        # 获取知识库当前激活版本
        kb = conn.execute(
            """
            SELECT active_version_id
            FROM knowledge_bases
            WHERE id = ?
            """,
            (kb_id,),
        ).fetchone()

        assert kb is not None
        assert kb["active_version_id"] == version_v1

    finally:
        conn.close()

    # ==================================================
    # 7. V1 完成后，新增文档 B
    # ==================================================

    content_b = """
    RabbitMQ 是一种消息队列系统。
    它可以通过消息传递实现不同服务之间的异步通信。
    在 Agent 工作流系统中，可以使用 RabbitMQ 分发执行任务。
    Go 服务负责工作流调度，Python 服务负责 Agent 内部执行。
    Redis 可以用于缓存和限流，SQLite 可以记录任务状态。
    """ * 15

    document_b_id = create_document(
        db_path=db_path,
        kb_id=kb_id,
        name="agent-infrastructure.txt",
        content=content_b,
    )

    # ==================================================
    # 8. 统计 V2 构建期间的 Embedding 调用
    # ==================================================

    original_embed_texts = embedder.embed_texts

    embedding_calls = []

    def tracked_embed_texts(texts):
        embedding_calls.append(len(texts))
        return original_embed_texts(texts)

    monkeypatch.setattr(
        embedder,
        "embed_texts",
        tracked_embed_texts,
    )

    # 计算文档 B 理论上需要生成多少个向量
    chunks_b = split_text(
        content_b,
        chunk_size=config.chunk_size,
        overlap=config.overlap,
    )

    assert chunks_b

    # ==================================================
    # 9. 创建 V2 索引任务
    # ==================================================

    task_v2 = create_index_task(
        db_path=db_path,
        kb_id=kb_id,
        pipeline_config=config,
    )

    # 从知识库读取当前 Active Version，作为 V2 的源版本
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        kb = conn.execute(
            """
            SELECT active_version_id
            FROM knowledge_bases
            WHERE id = ?
            """,
            (kb_id,),
        ).fetchone()

        assert kb is not None
        source_version_id = kb["active_version_id"]

    finally:
        conn.close()

    assert source_version_id == version_v1

    # ==================================================
    # 10. 构建 V2：复用 A，新增 B
    # ==================================================

    result_v2 = index_service.build_index(
        task_id=task_v2,
        source_version_id=source_version_id,
    )

    version_v2 = result_v2.version_id

    assert version_v2 != version_v1

    assert result_v2.document_count == 2

    assert result_v2.chunk_count == (
        result_v1.chunk_count + len(chunks_b)
    )

    # ==================================================
    # 11. 验证 Embedding 调用次数
    # ==================================================

    # V2 中 A 应该复用 V1 的向量。
    # 因此只有 B 应该调用真实 Embedding 模型。
    assert embedding_calls == [len(chunks_b)]

    # ==================================================
    # 12. 验证 V2 的向量记录
    # ==================================================

    v2_records = get_version_records(
        vector_store,
        version_v2,
    )

    assert len(v2_records) == result_v2.vector_count

    v2_a_vectors = {
        record["chunk_index"]: list(record["vector"])
        for record in v2_records
        if record["document_id"] == document_a_id
    }

    v2_b_vectors = [
        record
        for record in v2_records
        if record["document_id"] == document_b_id
    ]

    # A 的向量在 V2 中完整存在
    assert v2_a_vectors == v1_a_vectors

    # B 的向量已经生成
    assert len(v2_b_vectors) == len(chunks_b)

    # V2 总向量数量 = A 复用数量 + B 新生成数量
    assert result_v2.vector_count == (
        len(v1_a_vectors) + len(chunks_b)
    )

    # ==================================================
    # 13. 验证 V1 历史版本没有被修改
    # ==================================================

    v1_records_after = get_version_records(
        vector_store,
        version_v1,
    )

    v1_a_vectors_after = {
        record["chunk_index"]: list(record["vector"])
        for record in v1_records_after
        if record["document_id"] == document_a_id
    }

    assert v1_a_vectors_after == v1_a_vectors

    # V1 中不应该出现文档 B
    assert all(
        record["document_id"] != document_b_id
        for record in v1_records_after
    )

    # ==================================================
    # 14. 检查 V2 的 SQLite 状态和快照
    # ==================================================

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        task = conn.execute(
            """
            SELECT status, version_id
            FROM index_tasks
            WHERE id = ?
            """,
            (task_v2,),
        ).fetchone()

        version = conn.execute(
            """
            SELECT status, pipeline_hash
            FROM index_versions
            WHERE id = ?
            """,
            (version_v2,),
        ).fetchone()

        assert task is not None
        assert version is not None

        assert task["status"] == "ready"
        assert task["version_id"] == version_v2

        assert version["status"] == "ready"
        assert version["pipeline_hash"] == config.calculate_hash()

        snapshot = conn.execute(
            """
            SELECT document_id
            FROM version_documents
            WHERE version_id = ?
            """,
            (version_v2,),
        ).fetchall()

        snapshot_ids = {
            row["document_id"]
            for row in snapshot
        }

        assert snapshot_ids == {
            document_a_id,
            document_b_id,
        }

        kb = conn.execute(
            """
            SELECT active_version_id
            FROM knowledge_bases
            WHERE id = ?
            """,
            (kb_id,),
        ).fetchone()

        assert kb is not None
        assert kb["active_version_id"] == version_v2

    finally:
        conn.close()

    # ==================================================
    # 15. 验证 V1 和 V2 可以独立进行向量检索
    # ==================================================

    # 查询 A
    query_a = embedder.embed_query(
        "什么是 Agent？"
    )

    results_v1 = vector_store.search(
        query_vector=query_a,
        version_id=version_v1,
        limit=5,
    )

    results_v2_a = vector_store.search(
        query_vector=query_a,
        version_id=version_v2,
        limit=5,
    )

    assert results_v1
    assert results_v2_a

    assert all(
        result["version_id"] == version_v1
        for result in results_v1
    )

    assert all(
        result["version_id"] == version_v2
        for result in results_v2_a
    )

    assert any(
        result["document_id"] == document_a_id
        for result in results_v1
    )

    assert any(
        result["document_id"] == document_a_id
        for result in results_v2_a
    )

    # 查询 B
    query_b = embedder.embed_query(
        "RabbitMQ 如何帮助 Agent 工作流系统？"
    )

    results_v2_b = vector_store.search(
        query_vector=query_b,
        version_id=version_v2,
        limit=5,
    )

    assert results_v2_b

    assert all(
        result["version_id"] == version_v2
        for result in results_v2_b
    )

    assert any(
        result["document_id"] == document_b_id
        for result in results_v2_b
    )
