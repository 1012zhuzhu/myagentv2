
import pytest

from app.infrastructure.database import (
    init_db,
    create_knowledge_base,
    create_document,
    create_index_task,
    start_index_task,
    finish_index_task,
    fail_index_task,
    get_knowledge_base,
)


def create_task(db_path, kb_id):
    return create_index_task(
        db_path,
        kb_id,
        "test-model",
        "pipeline-v1"
    )


def test_finish_index_task_activates_version(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    task_id = create_task(db_path, kb_id)

    # pending -> running
    start_index_task(db_path, task_id)

    # running -> ready，并激活 V1
    version_id = finish_index_task(db_path, task_id)

    kb = get_knowledge_base(db_path, kb_id)

    assert kb["active_version_id"] == version_id

    # 验证重复完成不允许
    with pytest.raises(ValueError):
        finish_index_task(db_path, task_id)


def test_failed_version_does_not_replace_active_version(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    # 先成功构建 V1
    task_v1 = create_task(db_path, kb_id)
    start_index_task(db_path, task_v1)
    version_v1 = finish_index_task(db_path, task_v1)

    kb = get_knowledge_base(db_path, kb_id)
    assert kb["active_version_id"] == version_v1

    # 再创建 V2，但构建失败
    task_v2 = create_task(db_path, kb_id)
    start_index_task(db_path, task_v2)

    version_v2 = fail_index_task(
        db_path,
        task_v2,
        "Embedding model unavailable"
    )

    kb = get_knowledge_base(db_path, kb_id)

    # V2 失败不能影响 V1
    assert kb["active_version_id"] == version_v1
    assert version_v2 != version_v1

    # 失败任务不能再次完成
    with pytest.raises(ValueError):
        finish_index_task(db_path, task_v2)


def test_older_version_finishing_late_does_not_replace_newer_active(
    tmp_path
):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    # 创建 V1、V2、V3
    task_v1 = create_task(db_path, kb_id)
    task_v2 = create_task(db_path, kb_id)
    task_v3 = create_task(db_path, kb_id)

    # V1 先完成
    start_index_task(db_path, task_v1)
    version_v1 = finish_index_task(db_path, task_v1)

    # V2、V3 同时处于 building
    start_index_task(db_path, task_v2)
    start_index_task(db_path, task_v3)

    # V3 先完成，成为 Active
    version_v3 = finish_index_task(db_path, task_v3)

    kb = get_knowledge_base(db_path, kb_id)
    assert kb["active_version_id"] == version_v3

    # V2 后完成，但不能覆盖 V3
    version_v2 = finish_index_task(db_path, task_v2)

    kb = get_knowledge_base(db_path, kb_id)

    assert version_v2 != version_v3
    assert kb["active_version_id"] == version_v3


def test_invalid_task_transition_is_rejected(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")
    task_id = create_task(db_path, kb_id)

    # pending 不能直接 finish
    with pytest.raises(ValueError):
        finish_index_task(db_path, task_id)

    # pending 不能直接 fail
    with pytest.raises(ValueError):
        fail_index_task(
            db_path,
            task_id,
            "Some error"
        )

    # 非空错误原因是必须的
    with pytest.raises(ValueError):
        fail_index_task(
            db_path,
            task_id,
            " "
        )



from app.infrastructure.database import recover_interrupted_tasks


def test_recover_interrupted_tasks(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    task_id = create_task(db_path, kb_id)

    # 模拟任务已经开始，但 Python 进程突然退出
    start_index_task(db_path, task_id)

    # 模拟服务重启后的恢复
    recovered = recover_interrupted_tasks(db_path)

    assert recovered == [task_id]

    # 复用现有查询，验证任务和版本状态
    import sqlite3

    conn = sqlite3.connect(db_path)

    try:
        task = conn.execute(
            "SELECT status FROM index_tasks WHERE id = ?",
            (task_id,)
        ).fetchone()

        version = conn.execute(
            """
            SELECT v.status
            FROM index_versions AS v
            JOIN index_tasks AS t ON t.version_id = v.id
            WHERE t.id = ?
            """,
            (task_id,)
        ).fetchone()

        assert task[0] == "failed"
        assert version[0] == "failed"

    finally:
        conn.close()


def test_recover_interrupted_tasks_is_idempotent(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")
    task_id = create_task(db_path, kb_id)

    start_index_task(db_path, task_id)

    # 第一次恢复
    recovered = recover_interrupted_tasks(db_path)
    assert recovered == [task_id]

    # 第二次恢复，不应重复处理
    recovered_again = recover_interrupted_tasks(db_path)
    assert recovered_again == []


def test_recovery_does_not_replace_active_version(tmp_path):
    db_path = str(tmp_path / "knowledge.db")

    init_db(db_path)

    kb_id = create_knowledge_base(db_path, "test-kb")

    create_document(
        db_path,
        kb_id,
        "document-a",
        "文档 A"
    )

    # 1. 先构建 V1，确保它成为 Active Version
    task_v1 = create_task(db_path, kb_id)

    start_index_task(db_path, task_v1)
    version_v1 = finish_index_task(db_path, task_v1)

    kb = get_knowledge_base(db_path, kb_id)
    assert kb["active_version_id"] == version_v1

    # 2. 创建 V2，并启动构建
    task_v2 = create_task(db_path, kb_id)
    start_index_task(db_path, task_v2)

    # 3. 模拟服务重启，恢复遗留任务
    recovered = recover_interrupted_tasks(db_path)

    assert recovered == [task_v2]

    # 4. 确认 V1 仍然是 Active Version
    kb = get_knowledge_base(db_path, kb_id)

    assert kb["active_version_id"] == version_v1
