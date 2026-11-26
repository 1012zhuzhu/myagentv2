import os
from pathlib import Path
from datetime import datetime

from app.infrastructure.database import (
    init_db,
    create_knowledge_base,
    create_document,
    create_index_task,
    get_active_version,
)
from app.rag.pipeline import PipelineConfig
from app.rag.setup import create_rag_system


BASE_DIR = Path(__file__).resolve().parents[1] / "python"

DB_PATH = BASE_DIR / "data" / "agent.db"
VECTOR_DB_PATH = BASE_DIR / "data" / "lancedb"
DOCUMENT_PATH = BASE_DIR / "data" / "employee_travel_policy.txt"


def main():
    # 1. 读取测试文本
    content = DOCUMENT_PATH.read_text(encoding="utf-8")

    # 2. 获取模型配置
    embedding_model = os.environ["EMBEDDING_MODEL"]
    embedding_model_path = os.environ["EMBEDDING_MODEL_PATH"]

    config = PipelineConfig(
        embedding_model=embedding_model,
        embedding_model_path=embedding_model_path,
    )

    # 3. 初始化数据库
    init_db(str(DB_PATH))

    # 4. 创建 RAG 系统
    _, index_service = create_rag_system(
        db_path=str(DB_PATH),
        vector_db_path=str(VECTOR_DB_PATH),
        embedding_model=embedding_model,
        embedding_model_path=embedding_model_path,
    )

    # 5. 创建知识库
    kb_name = (
        "RAG测试知识库-员工出差报销-"
        + datetime.now().strftime("%Y%m%d%H%M%S")
    )

    kb_id = create_knowledge_base(
        str(DB_PATH),
        kb_name,
    )

    # 6. 创建文档及其第一个修订版本
    document_id = create_document(
        db_path=str(DB_PATH),
        kb_id=kb_id,
        name="employee_travel_policy.txt",
        content=content,
    )

    # 7. 创建索引任务和版本快照
    task_id = create_index_task(
        db_path=str(DB_PATH),
        kb_id=kb_id,
        pipeline_config=config,
    )

    print(f"Knowledge Base ID: {kb_id}")
    print(f"Document ID: {document_id}")
    print(f"Index Task ID: {task_id}")

    # 8. 执行索引构建
    index_service.build_index(task_id)

    # 9. 检查 Active Version
    active_version = get_active_version(
        str(DB_PATH),
        kb_id,
    )

    if active_version is None:
        raise RuntimeError("Index built but no active version found")

    print("Index build completed!")
    print(f"Active Version ID: {active_version['id']}")


if __name__ == "__main__":
    main()