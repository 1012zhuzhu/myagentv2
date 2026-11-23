
from app.infrastructure.database import (
    get_connection,
    init_db,
    create_knowledge_base,
    create_document,
    update_document,
    create_index_task,
)

from app.rag.document_store import get_version_documents


def test_version_snapshot_is_immutable(tmp_path):
    db_path = str(tmp_path / "test.db")

    init_db(db_path)

    # 创建知识库和文档
    kb_id = create_knowledge_base(db_path, "test-kb")

    document_id = create_document(
        db_path,
        kb_id,
        "employee-handbook",
        "这是旧版本的员工手册"
    )

    # 创建 Version 1，此时固定 Revision 1
    task_id = create_index_task(
        db_path,
        kb_id,
        "test-model",
        "pipeline-v1"
    )

    # 查询任务对应的版本 ID
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT version_id FROM index_tasks WHERE id = ?",
            (task_id,)
        ).fetchone()
        version_id = row["version_id"]
    finally:
        conn.close()

    # 更新文档，产生 Revision 2
    update_document(
        db_path,
        document_id,
        "这是新版本的员工手册"
    )

    # 读取 Version 1 的快照
    documents = get_version_documents(db_path, version_id)

    assert len(documents) == 1
    assert documents[0].document_id == document_id
    assert documents[0].revision_id == 1
    assert documents[0].revision_number == 1
    assert documents[0].content == "这是旧版本的员工手册"
    assert documents[0].content_hash != ""


def test_two_versions_keep_independent_snapshots(tmp_path):
    db_path = str(tmp_path / "test_two_versions.db")

    # 1. 初始化数据库
    init_db(db_path)

    # 2. 创建知识库
    kb_id = create_knowledge_base(db_path, "test-kb")

    # 3. 创建两份文档
    doc_a = create_document(
        db_path,
        kb_id,
        "employee-handbook",
        "旧员工手册"
    )

    doc_b = create_document(
        db_path,
        kb_id,
        "travel-policy",
        "旧差旅政策"
    )

    # 4. 创建 Version 1
    task_v1 = create_index_task(
        db_path,
        kb_id,
        "test-model",
        "pipeline-v1"
    )

    # 查询 V1 的 version_id
    conn = get_connection(db_path)
    try:
        version_v1 = conn.execute(
            "SELECT version_id FROM index_tasks WHERE id = ?",
            (task_v1,)
        ).fetchone()["version_id"]
    finally:
        conn.close()

    # 5. 更新文档 A，产生 Revision 2
    update_document(
        db_path,
        doc_a,
        "新员工手册"
    )

    # 6. 创建 Version 2
    task_v2 = create_index_task(
        db_path,
        kb_id,
        "test-model",
        "pipeline-v1"
    )

    conn = get_connection(db_path)
    try:
        version_v2 = conn.execute(
            "SELECT version_id FROM index_tasks WHERE id = ?",
            (task_v2,)
        ).fetchone()["version_id"]
    finally:
        conn.close()

    # 7. 分别读取两个版本的快照
    documents_v1 = get_version_documents(db_path, version_v1)
    documents_v2 = get_version_documents(db_path, version_v2)

    # 转成 document_id -> 文档对象，方便断言
    snapshot_v1 = {
        doc.document_id: doc
        for doc in documents_v1
    }

    snapshot_v2 = {
        doc.document_id: doc
        for doc in documents_v2
    }

    # 8. 验证两个版本都包含两份文档
    assert len(snapshot_v1) == 2
    assert len(snapshot_v2) == 2

    # 9. 验证 V1 保留旧员工手册
    assert snapshot_v1[doc_a].content == "旧员工手册"
    assert snapshot_v1[doc_a].revision_number == 1

    # 10. 验证 V2 使用新员工手册
    assert snapshot_v2[doc_a].content == "新员工手册"
    assert snapshot_v2[doc_a].revision_number == 2

    # 11. 验证差旅政策没有修改，两个版本都使用 Revision 1
    assert snapshot_v1[doc_b].content == "旧差旅政策"
    assert snapshot_v2[doc_b].content == "旧差旅政策"

    assert snapshot_v1[doc_b].revision_number == 1
    assert snapshot_v2[doc_b].revision_number == 1

    # 12. 验证两个版本的员工手册修订 ID 不同
    assert (
        snapshot_v1[doc_a].revision_id
        != snapshot_v2[doc_a].revision_id
    )
