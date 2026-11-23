
import hashlib
import sqlite3

from app.rag.pipeline import PipelineConfig


# =========================
# 1. 数据库连接
# =========================

def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# =========================
# 2. 数据库初始化与迁移
# =========================

def init_db(db_path: str) -> None:
    conn = get_connection(db_path)

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_bases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                active_version_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                knowledge_base_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at TEXT DEFAULT NULL,

                FOREIGN KEY (knowledge_base_id)
                    REFERENCES knowledge_bases(id),

                UNIQUE (knowledge_base_id, name)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                revision_number INTEGER NOT NULL,
                content TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (document_id)
                    REFERENCES knowledge_documents(id),

                UNIQUE (document_id, revision_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                knowledge_base_id INTEGER NOT NULL,
                version_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                pipeline_hash TEXT NOT NULL,
                pipeline_config_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (knowledge_base_id)
                    REFERENCES knowledge_bases(id),

                UNIQUE (knowledge_base_id, version_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS version_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                document_id INTEGER NOT NULL,
                revision_id INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                pipeline_hash TEXT NOT NULL,
                chunk_count INTEGER NOT NULL DEFAULT 0,

                FOREIGN KEY (version_id)
                    REFERENCES index_versions(id),

                FOREIGN KEY (document_id)
                    REFERENCES knowledge_documents(id),

                FOREIGN KEY (revision_id)
                    REFERENCES document_revisions(id),

                UNIQUE (version_id, document_id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (version_id)
                    REFERENCES index_versions(id)
            )
        """)

        # 兼容旧数据库：为 knowledge_documents 增加 deleted_at
        document_columns = conn.execute(
            "PRAGMA table_info(knowledge_documents)"
        ).fetchall()

        document_column_names = {
            column["name"] for column in document_columns
        }

        if "deleted_at" not in document_column_names:
            conn.execute("""
                ALTER TABLE knowledge_documents
                ADD COLUMN deleted_at TEXT DEFAULT NULL
            """)

        # 兼容旧数据库：为 index_versions 增加配置快照字段
        version_columns = conn.execute(
            "PRAGMA table_info(index_versions)"
        ).fetchall()

        version_column_names = {
            column["name"] for column in version_columns
        }

        if "pipeline_config_json" not in version_column_names:
            conn.execute("""
                ALTER TABLE index_versions
                ADD COLUMN pipeline_config_json TEXT NOT NULL DEFAULT '{}'
            """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 3. 创建知识库
# =========================

def create_knowledge_base(db_path: str, name: str) -> int:
    conn = get_connection(db_path)

    try:
        cursor = conn.execute(
            "INSERT INTO knowledge_bases (name) VALUES (?)",
            (name,)
        )
        conn.commit()
        return cursor.lastrowid

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 4. 查询知识库
# =========================

def get_knowledge_base(
    db_path: str,
    kb_id: int
) -> sqlite3.Row | None:
    conn = get_connection(db_path)

    try:
        return conn.execute(
            "SELECT * FROM knowledge_bases WHERE id = ?",
            (kb_id,)
        ).fetchone()

    finally:
        conn.close()


# =========================
# 5. 查询 Active Version
# =========================

def get_active_version(
    db_path: str,
    kb_id: int
) -> sqlite3.Row | None:
    conn = get_connection(db_path)

    try:
        return conn.execute(
            """
            SELECT v.*
            FROM knowledge_bases AS kb
            JOIN index_versions AS v
                ON v.id = kb.active_version_id
            WHERE kb.id = ?
              AND v.knowledge_base_id = kb.id
              AND v.status = 'ready'
            """,
            (kb_id,)
        ).fetchone()

    finally:
        conn.close()


# =========================
# 6. 版本安全检查
# =========================

def assert_version_is_not_active(
    db_path: str,
    version_id: int,
) -> None:
    """
    检查版本是否允许执行失败向量清理。

    规则：
    1. 版本必须存在。
    2. 版本不能是 Active Version。
    3. 版本必须处于 failed 状态。

    只有通过检查，调用方才能继续删除对应向量。
    """
    if version_id <= 0:
        raise ValueError("version_id must be greater than 0")

    conn = get_connection(db_path)

    try:
        row = conn.execute(
            """
            SELECT
                v.id AS version_id,
                v.status,
                kb.active_version_id
            FROM index_versions AS v
            JOIN knowledge_bases AS kb
                ON kb.id = v.knowledge_base_id
            WHERE v.id = ?
            """,
            (version_id,)
        ).fetchone()

        if row is None:
            raise ValueError(
                f"Index version {version_id} not found"
            )

        if row["active_version_id"] == version_id:
            raise RuntimeError(
                f"Cannot delete vectors of active version {version_id}"
            )

        if row["status"] != "failed":
            raise RuntimeError(
                f"Cannot clean vectors of version {version_id}: "
                f"status is {row['status']}, expected failed"
            )

    finally:
        conn.close()


# =========================
# 7. 文档 Hash
# =========================

def calculate_content_hash(content: str) -> str:
    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()


# =========================
# 8. 创建文档
# =========================

def create_document(
    db_path: str,
    kb_id: int,
    name: str,
    content: str
) -> int:
    conn = get_connection(db_path)

    try:
        content_hash = calculate_content_hash(content)

        cursor = conn.execute(
            """
            INSERT INTO knowledge_documents (
                knowledge_base_id,
                name,
                content,
                content_hash
            )
            VALUES (?, ?, ?, ?)
            """,
            (kb_id, name, content, content_hash)
        )

        document_id = cursor.lastrowid

        conn.execute(
            """
            INSERT INTO document_revisions (
                document_id,
                revision_number,
                content,
                content_hash
            )
            VALUES (?, ?, ?, ?)
            """,
            (document_id, 1, content, content_hash)
        )

        conn.commit()
        return document_id

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 9. 更新文档
# =========================

def update_document(
    db_path: str,
    document_id: int,
    new_content: str
) -> int:
    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        document = conn.execute(
            """
            SELECT *
            FROM knowledge_documents
            WHERE id = ?
              AND deleted_at IS NULL
            """,
            (document_id,)
        ).fetchone()

        if document is None:
            raise ValueError("Document not found or already deleted")

        new_content_hash = calculate_content_hash(new_content)

        current_revision = conn.execute(
            """
            SELECT MAX(revision_number)
            FROM document_revisions
            WHERE document_id = ?
            """,
            (document_id,)
        ).fetchone()[0]

        if document["content_hash"] == new_content_hash:
            conn.commit()
            return current_revision

        next_revision_number = current_revision + 1

        conn.execute(
            """
            UPDATE knowledge_documents
            SET content = ?,
                content_hash = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_content, new_content_hash, document_id)
        )

        conn.execute(
            """
            INSERT INTO document_revisions (
                document_id,
                revision_number,
                content,
                content_hash
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                document_id,
                next_revision_number,
                new_content,
                new_content_hash
            )
        )

        conn.commit()
        return next_revision_number

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 10. 软删除文档
# =========================

def delete_document(db_path: str, document_id: int) -> bool:
    """
    软删除文档。

    True：本次成功执行删除
    False：文档此前已经删除

    不删除 document_revisions 和历史版本快照。
    """
    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        document = conn.execute(
            """
            SELECT id, deleted_at
            FROM knowledge_documents
            WHERE id = ?
            """,
            (document_id,)
        ).fetchone()

        if document is None:
            raise ValueError("Document not found")

        if document["deleted_at"] is not None:
            conn.commit()
            return False

        conn.execute(
            """
            UPDATE knowledge_documents
            SET deleted_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND deleted_at IS NULL
            """,
            (document_id,)
        )

        conn.commit()
        return True

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 11. 恢复文档
# =========================

def restore_document(db_path: str, document_id: int) -> bool:
    """
    恢复软删除文档。

    True：本次成功恢复
    False：文档此前就没有被删除
    """
    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        document = conn.execute(
            """
            SELECT id, deleted_at
            FROM knowledge_documents
            WHERE id = ?
            """,
            (document_id,)
        ).fetchone()

        if document is None:
            raise ValueError("Document not found")

        if document["deleted_at"] is None:
            conn.commit()
            return False

        conn.execute(
            """
            UPDATE knowledge_documents
            SET deleted_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND deleted_at IS NOT NULL
            """,
            (document_id,)
        )

        conn.commit()
        return True

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 12. 查询文档
# =========================

def get_document(
    db_path: str,
    document_id: int,
    include_deleted: bool = False
) -> sqlite3.Row | None:
    conn = get_connection(db_path)

    try:
        if include_deleted:
            return conn.execute(
                """
                SELECT *
                FROM knowledge_documents
                WHERE id = ?
                """,
                (document_id,)
            ).fetchone()

        return conn.execute(
            """
            SELECT *
            FROM knowledge_documents
            WHERE id = ?
              AND deleted_at IS NULL
            """,
            (document_id,)
        ).fetchone()

    finally:
        conn.close()


# =========================
# 13. 查询文档历史修订
# =========================

def get_document_revisions(
    db_path: str,
    document_id: int
) -> list[sqlite3.Row]:
    conn = get_connection(db_path)

    try:
        return conn.execute(
            """
            SELECT *
            FROM document_revisions
            WHERE document_id = ?
            ORDER BY revision_number DESC
            """,
            (document_id,)
        ).fetchall()

    finally:
        conn.close()


# =========================
# 14. 创建索引构建任务
# =========================

def create_index_task(
    db_path: str,
    kb_id: int,
    pipeline_config: PipelineConfig
) -> int:
    embedding_model = pipeline_config.embedding_model
    pipeline_hash = pipeline_config.calculate_hash()
    pipeline_config_json = pipeline_config.to_json()

    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        kb = conn.execute(
            """
            SELECT id
            FROM knowledge_bases
            WHERE id = ?
            """,
            (kb_id,)
        ).fetchone()

        if kb is None:
            raise ValueError("Knowledge base not found")

        result = conn.execute(
            """
            SELECT MAX(version_number) AS max_version
            FROM index_versions
            WHERE knowledge_base_id = ?
            """,
            (kb_id,)
        ).fetchone()

        current_version = result["max_version"] or 0
        next_version_number = current_version + 1

        cursor = conn.execute(
            """
            INSERT INTO index_versions (
                knowledge_base_id,
                version_number,
                status,
                embedding_model,
                pipeline_hash,
                pipeline_config_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                kb_id,
                next_version_number,
                "pending",
                embedding_model,
                pipeline_hash,
                pipeline_config_json,
            )
        )

        version_id = cursor.lastrowid

        cursor = conn.execute(
            """
            INSERT INTO index_tasks (version_id, status)
            VALUES (?, ?)
            """,
            (version_id, "pending")
        )

        task_id = cursor.lastrowid

        # 固定当前未删除文档的最新 Revision 快照
        documents = conn.execute(
            """
            SELECT
                d.id AS document_id,
                r.id AS revision_id,
                r.content_hash
            FROM knowledge_documents AS d
            JOIN document_revisions AS r
                ON r.id = (
                    SELECT r2.id
                    FROM document_revisions AS r2
                    WHERE r2.document_id = d.id
                    ORDER BY r2.revision_number DESC
                    LIMIT 1
                )
            WHERE d.knowledge_base_id = ?
              AND d.deleted_at IS NULL
            """,
            (kb_id,)
        ).fetchall()

        for document in documents:
            conn.execute(
                """
                INSERT INTO version_documents (
                    version_id,
                    document_id,
                    revision_id,
                    content_hash,
                    pipeline_hash
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    document["document_id"],
                    document["revision_id"],
                    document["content_hash"],
                    pipeline_hash,
                )
            )

        conn.commit()
        return task_id

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 15. 启动索引构建任务
# =========================

def start_index_task(db_path: str, task_id: int) -> None:
    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        task = conn.execute(
            """
            SELECT
                t.version_id,
                t.status AS task_status,
                v.status AS version_status
            FROM index_tasks AS t
            JOIN index_versions AS v
                ON v.id = t.version_id
            WHERE t.id = ?
            """,
            (task_id,)
        ).fetchone()

        if task is None:
            raise ValueError("Index task not found")

        if task["task_status"] != "pending":
            raise ValueError(
                f"Task cannot start from status: {task['task_status']}"
            )

        if task["version_status"] != "pending":
            raise ValueError(
                f"Version cannot start from status: {task['version_status']}"
            )

        cursor = conn.execute(
            """
            UPDATE index_tasks
            SET status = 'running',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND status = 'pending'
            """,
            (task_id,)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to start index task")

        cursor = conn.execute(
            """
            UPDATE index_versions
            SET status = 'building'
            WHERE id = ?
              AND status = 'pending'
            """,
            (task["version_id"],)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to start index version")

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 16. 索引构建成功
# =========================

def finish_index_task(db_path: str, task_id: int) -> int:
    """
    将索引任务和版本标记为 ready，并尝试切换 Active Version。
    只能在向量索引成功构建并持久化之后调用。
    """
    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        task = conn.execute(
            """
            SELECT
                t.id AS task_id,
                t.status AS task_status,
                t.version_id,
                v.status AS version_status,
                v.version_number,
                v.knowledge_base_id,
                kb.active_version_id
            FROM index_tasks AS t
            JOIN index_versions AS v
                ON v.id = t.version_id
            JOIN knowledge_bases AS kb
                ON kb.id = v.knowledge_base_id
            WHERE t.id = ?
            """,
            (task_id,)
        ).fetchone()

        if task is None:
            raise ValueError("Index task not found")

        if task["task_status"] != "running":
            raise ValueError(
                f"Task cannot finish from status: {task['task_status']}"
            )

        if task["version_status"] != "building":
            raise ValueError(
                f"Version cannot finish from status: {task['version_status']}"
            )

        version_id = task["version_id"]

        cursor = conn.execute(
            """
            UPDATE index_tasks
            SET status = 'ready',
                error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND status = 'running'
            """,
            (task_id,)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to finish index task")

        cursor = conn.execute(
            """
            UPDATE index_versions
            SET status = 'ready'
            WHERE id = ?
              AND status = 'building'
            """,
            (version_id,)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to finish index version")

        active_version_id = task["active_version_id"]
        should_activate = False

        if active_version_id is None:
            should_activate = True
        else:
            active_version = conn.execute(
                """
                SELECT version_number, status
                FROM index_versions
                WHERE id = ?
                  AND knowledge_base_id = ?
                """,
                (active_version_id, task["knowledge_base_id"])
            ).fetchone()

            if active_version is None:
                raise RuntimeError("Active version reference is invalid")

            if active_version["status"] != "ready":
                raise RuntimeError("Current active version is not ready")

            if task["version_number"] > active_version["version_number"]:
                should_activate = True

        if should_activate:
            conn.execute(
                """
                UPDATE knowledge_bases
                SET active_version_id = ?
                WHERE id = ?
                """,
                (version_id, task["knowledge_base_id"])
            )

        conn.commit()
        return version_id

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 17. 索引构建失败
# =========================

def fail_index_task(
    db_path: str,
    task_id: int,
    error_message: str
) -> int:
    """
    将正在执行的索引任务和版本标记为 failed。
    不修改 knowledge_bases.active_version_id。
    """
    if not error_message or not error_message.strip():
        raise ValueError("Error message cannot be empty")

    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        task = conn.execute(
            """
            SELECT
                t.id AS task_id,
                t.status AS task_status,
                t.version_id,
                v.status AS version_status
            FROM index_tasks AS t
            JOIN index_versions AS v
                ON v.id = t.version_id
            WHERE t.id = ?
            """,
            (task_id,)
        ).fetchone()

        if task is None:
            raise ValueError("Index task not found")

        if task["task_status"] != "running":
            raise ValueError(
                f"Task cannot fail from status: {task['task_status']}"
            )

        if task["version_status"] != "building":
            raise ValueError(
                f"Version cannot fail from status: {task['version_status']}"
            )

        version_id = task["version_id"]

        cursor = conn.execute(
            """
            UPDATE index_tasks
            SET status = 'failed',
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND status = 'running'
            """,
            (error_message, task_id)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to mark index task as failed")

        cursor = conn.execute(
            """
            UPDATE index_versions
            SET status = 'failed'
            WHERE id = ?
              AND status = 'building'
            """,
            (version_id,)
        )

        if cursor.rowcount != 1:
            raise RuntimeError("Failed to mark index version as failed")

        conn.commit()
        return version_id

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# =========================
# 18. 服务启动时恢复中断任务
# =========================

def recover_interrupted_tasks(db_path: str) -> list[int]:
    """
    服务启动时，将遗留的 running 任务收敛为 failed。

    1. pending 任务不处理。
    2. running 任务及其 building 版本标记为 failed。
    3. 不修改 Active Version。
    4. 状态更新在同一事务完成。
    5. 重复调用安全。
    """
    error_message = (
        "Interrupted by process restart; "
        "the index task was still running during startup recovery."
    )

    conn = get_connection(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")

        tasks = conn.execute(
            """
            SELECT
                t.id AS task_id,
                t.version_id,
                t.status AS task_status,
                v.status AS version_status
            FROM index_tasks AS t
            JOIN index_versions AS v
                ON v.id = t.version_id
            WHERE t.status = 'running'
            ORDER BY t.id
            """
        ).fetchall()

        if not tasks:
            conn.commit()
            return []

        for task in tasks:
            if task["version_status"] != "building":
                raise RuntimeError(
                    f"Inconsistent index state: "
                    f"task {task['task_id']} is running, "
                    f"but version {task['version_id']} "
                    f"is {task['version_status']}"
                )

        task_ids: list[int] = []
        version_ids: set[int] = set()

        for task in tasks:
            cursor = conn.execute(
                """
                UPDATE index_tasks
                SET status = 'failed',
                    error_message = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND status = 'running'
                """,
                (error_message, task["task_id"])
            )

            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Failed to recover task {task['task_id']}"
                )

            task_ids.append(task["task_id"])
            version_ids.add(task["version_id"])

        for version_id in version_ids:
            cursor = conn.execute(
                """
                UPDATE index_versions
                SET status = 'failed'
                WHERE id = ?
                  AND status = 'building'
                """,
                (version_id,)
            )

            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Failed to recover version {version_id}"
                )

        conn.commit()
        return task_ids

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
