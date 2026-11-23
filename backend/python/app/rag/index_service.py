
import logging

from app.infrastructure.database import (
    start_index_task,
    finish_index_task,
    fail_index_task,
    assert_version_is_not_active,
)
from app.rag.index_builder import IndexBuilder
from app.rag.models import IndexBuildResult
from app.rag.vector_store import VectorStore


logger = logging.getLogger(__name__)


class IndexService:
    """
    索引任务协调服务。

    负责：
    1. 启动数据库中的索引任务。
    2. 调用 IndexBuilder 构建向量。
    3. 构建成功后完成任务并切换 Active Version。
    4. 构建失败后标记任务失败。
    5. 清理失败版本遗留的向量。

    不负责：
    - 文档快照创建
    - 文本切分
    - Embedding
    - LanceDB 底层读写实现
    """

    def __init__(
        self,
        db_path: str,
        index_builder: IndexBuilder,
        vector_store: VectorStore,
    ):
        self.db_path = db_path
        self.index_builder = index_builder
        self.vector_store = vector_store

    def build_index(
        self,
        task_id: int,
        source_version_id: int | None = None,
    ) -> IndexBuildResult:
        if task_id <= 0:
            raise ValueError("task_id must be greater than 0")

        # 1. 启动任务：pending -> running
        start_index_task(
            self.db_path,
            task_id,
        )

        # 2. 查询任务对应的 version_id
        from app.infrastructure.database import get_connection

        conn = get_connection(self.db_path)

        try:
            task = conn.execute(
                """
                SELECT version_id
                FROM index_tasks
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()

            if task is None:
                raise ValueError(
                    f"Index task {task_id} not found"
                )

            target_version_id = task["version_id"]

        finally:
            conn.close()

        try:
            # 3. 执行实际向量构建
            result = self.index_builder.build_version(
                db_path=self.db_path,
                target_version_id=target_version_id,
                source_version_id=source_version_id,
            )

            # 4. 构建成功：running -> ready
            finished_version_id = finish_index_task(
                self.db_path,
                task_id,
            )

            if finished_version_id != result.version_id:
                raise RuntimeError(
                    "Finished version ID does not match build result"
                )

            return result

        except Exception as build_error:
            # 5. 构建或提交失败，尝试将任务标记为 failed
            try:
                version_id = fail_index_task(
                    self.db_path,
                    task_id,
                    str(build_error) or type(build_error).__name__,
                )

            except Exception:
                logger.exception(
                    "Failed to mark index task %s as failed",
                    task_id,
                )
                raise build_error

            # 6. 确认候选版本不是 Active，且状态为 failed
            try:
                assert_version_is_not_active(
                    self.db_path,
                    version_id,
                )

                # 7. 删除失败版本的残留向量
                self.vector_store.delete_version(
                    version_id,
                )

            except Exception:
                # 清理失败不能覆盖最初的构建异常。
                # 后续可通过失败版本清理任务再次处理。
                logger.exception(
                    "Failed to clean vectors for version %s",
                    version_id,
                )

            raise build_error
