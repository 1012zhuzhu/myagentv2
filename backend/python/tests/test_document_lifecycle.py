
from app.rag.pipeline import PipelineConfig
from app.infrastructure.database import (
    init_db,
    create_knowledge_base,
    create_document,
    delete_document,
    restore_document,
    get_document,
    get_document_revisions,
    create_index_task,
)

from app.rag.document_store import get_version_documents


        
def create_test_pipeline_config():
    return PipelineConfig(
        embedding_model="test-model",
        embedding_model_path="test-model-path",
        chunk_size=80,
        overlap=20,
    )
        

def create_test_index_task(db_path, kb_id):
    return create_index_task(
        db_path,
        kb_id,
        create_test_pipeline_config(),
    )


def test_delete_document_preserves_history(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")
    document_id = create_document(
        db_path,
        kb_id,
        "employee-handbook",
        "员工手册第一版"
    )

    # 创建 V1，固定文档快照
    create_test_index_task(db_path, kb_id)

    # 执行软删除
    assert delete_document(db_path, document_id) is True

    # 重复删除应该是幂等的
    assert delete_document(db_path, document_id) is False

    # 默认查询看不到已删除文档
    assert get_document(db_path, document_id) is None

    # 管理查询仍然可以看到文档记录
    deleted_document = get_document(
        db_path,
        document_id,
        include_deleted=True
    )

    assert deleted_document is not None
    assert deleted_document["deleted_at"] is not None

    # 历史 Revision 仍然存在
    revisions = get_document_revisions(db_path, document_id)

    assert len(revisions) == 1
    assert revisions[0]["content"] == "员工手册第一版"

    # V1 的历史快照仍然能读取
    v1_documents = get_version_documents(db_path, 1)

    assert len(v1_documents) == 1
    assert v1_documents[0].document_id == document_id
    assert v1_documents[0].content == "员工手册第一版"


def test_new_version_excludes_deleted_document(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    document_a = create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    document_b = create_document(
        db_path,
        kb_id,
        "document-b",
        "文档 B"
    )

    # V1 包含 A、B
    create_test_index_task(db_path, kb_id)

    # 删除 A
    delete_document(db_path, document_a)

    # V2 应该只包含 B
    create_test_index_task(db_path, kb_id)

    v1_documents = get_version_documents(db_path, 1)
    v2_documents = get_version_documents(db_path, 2)

    assert {item.document_id for item in v1_documents} == {
        document_a,
        document_b
    }

    assert {item.document_id for item in v2_documents} == {
        document_b
    }

    # V1 中的 A 仍然保留原始内容
    old_document_a = next(
        item for item in v1_documents
        if item.document_id == document_a
    )

    assert old_document_a.content == "文档 A"


def test_restored_document_is_included_in_new_version(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    document_a = create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    document_b = create_document(
        db_path,
        kb_id,
        "document-b",
        "文档 B"
    )

    # V1：A、B
    create_test_index_task(db_path, kb_id)

    # 删除 A，创建 V2
    delete_document(db_path, document_a)

    create_test_index_task(db_path, kb_id)

    # 恢复 A
    assert restore_document(db_path, document_a) is True

    # 重复恢复应该是幂等的
    assert restore_document(db_path, document_a) is False

    # V3：A、B 重新纳入
    create_test_index_task(db_path, kb_id)

    v2_documents = get_version_documents(db_path, 2)
    v3_documents = get_version_documents(db_path, 3)

    assert {item.document_id for item in v2_documents} == {
        document_b
    }

    assert {item.document_id for item in v3_documents} == {
        document_a,
        document_b
    }

    # 恢复没有创建新的 Revision
    revisions = get_document_revisions(db_path, document_a)
    assert len(revisions) == 1
