
from pathlib import Path
import uuid

from app.infrastructure.database import (
    init_db,
    create_knowledge_base,
    create_document,
    create_index_task,
    start_index_task,
    get_connection,
)

# 每次测试创建独立数据库
DB_PATH = Path("data") / f"test_e09_{uuid.uuid4().hex[:8]}.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DB_PATH = str(DB_PATH)



def main():
    # 1. 初始化数据库
    init_db(DB_PATH)

    # 2. 创建一个测试知识库
    kb_id = create_knowledge_base(
        DB_PATH,
        "E09测试知识库"
    )

    # 3. 添加两篇测试文档
    create_document(
        DB_PATH,
        kb_id,
        "员工手册",
        "员工手册第一版内容"
    )

    create_document(
        DB_PATH,
        kb_id,
        "请假制度",
        "请假制度第一版内容"
    )

    # 4. 创建索引任务
    task_id = create_index_task(
        DB_PATH,
        kb_id,
        "test-embedding-model",
        "pipeline-v1"
    )

    print("创建任务成功:", task_id)

    # 5. 查询创建结果
    conn = get_connection(DB_PATH)

    try:
        print("索引版本:")
        versions = conn.execute(
            "SELECT * FROM index_versions"
        ).fetchall()

        for version in versions:
            print(dict(version))

        print("版本文档快照:")
        documents = conn.execute(
            "SELECT * FROM version_documents"
        ).fetchall()

        for document in documents:
            print(dict(document))

    finally:
        conn.close()

    # 6. 启动任务
    start_index_task(DB_PATH, task_id)

    conn = get_connection(DB_PATH)

    try:
        task = conn.execute(
            "SELECT * FROM index_tasks WHERE id = ?",
            (task_id,)
        ).fetchone()

        version = conn.execute(
            "SELECT * FROM index_versions WHERE id = ?",
            (1,)
        ).fetchone()

        print("启动后的任务状态:", task["status"])
        print("启动后的版本状态:", version["status"])

    finally:
        conn.close()

        print("任务启动成功:", task_id)


if __name__ == "__main__":
    main()
