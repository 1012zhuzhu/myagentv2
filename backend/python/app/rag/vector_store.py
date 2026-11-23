
import lancedb

from app.rag.models import VectorRecord, build_vector_id


class VectorStore:
    def __init__(
        self,
        db_url: str,
        table_name: str = "knowledge_vectors",
    ):
        self.db = lancedb.connect(db_url)
        self.table_name = table_name

    @staticmethod
    def _record_to_dict(record: VectorRecord) -> dict:
        """将 VectorRecord 转换为 LanceDB 可写入的数据格式。"""
        return {
            "id": record.id,
            "version_id": record.version_id,
            "document_id": record.document_id,
            "revision_id": record.revision_id,
            "chunk_index": record.chunk_index,
            "content_hash": record.content_hash,
            "pipeline_hash": record.pipeline_hash,
            "text": record.text,
            "vector": record.vector,
        }

    
    def add_records(
        self,
        records: list[VectorRecord],
    ) -> None:
        """
        幂等写入向量记录。

        1. 新 ID：插入。
        2. 已存在且内容一致：跳过。
        3. 已存在但内容不一致：抛出异常。
        """

        if not records:
            return

        # 1. 检查本次批次内部是否有重复 ID
        incoming: dict[str, VectorRecord] = {}

        for record in records:
            if record.id in incoming:
                raise RuntimeError(
                    f"Duplicate vector ID in incoming records: {record.id}"
                )

            incoming[record.id] = record

        # 2. LanceDB 表不存在时，直接创建
        if self.table_name not in self.db.table_names():
            self.db.create_table(
                self.table_name,
                [
                    self._record_to_dict(record)
                    for record in records
                ],
            )
            return

        table = self.db.open_table(self.table_name)

        # 3. 读取现有记录
        arrow_table = table.to_arrow()
        existing_records = arrow_table.to_pylist()

        # 4. 建立现有 ID 索引
        existing_by_id = {
            row["id"]: row
            for row in existing_records
            if row["id"] in incoming
        }

        # 5. 检查重复 ID 是否内容一致
        new_records = []

        for record_id, record in incoming.items():
            existing = existing_by_id.get(record_id)

            if existing is None:
                new_records.append(record)
                continue

            expected = self._record_to_dict(record)

            # 比较除 vector 外的所有字段
            fields_to_compare = [
                "version_id",
                "document_id",
                "revision_id",
                "chunk_index",
                "content_hash",
                "pipeline_hash",
                "text",
            ]

            for field in fields_to_compare:
                if existing[field] != expected[field]:
                    raise RuntimeError(
                        f"Vector ID conflict: {record_id}, "
                        f"field={field}"
                    )

            # 向量也必须一致
            if list(existing["vector"]) != list(expected["vector"]):
                raise RuntimeError(
                    f"Vector ID conflict: {record_id}, "
                    "field=vector"
                )

        # 6. 只插入不存在的新记录
        if new_records:
            table.add(
                [
                    self._record_to_dict(record)
                    for record in new_records
                ]
            )

    def search(
        self,
        query_vector: list[float],
        version_id: int,
        limit: int = 5,
    ) -> list[dict]:
        """只检索指定版本的向量。"""

        if version_id < 0:
            raise ValueError("version_id cannot be negative")

        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        if not self.table_name in self.db.table_names():
            return []

        table = self.db.open_table(self.table_name)

        results = (
            table.search(query_vector)
            .where(f"version_id = {version_id}")
            .limit(limit)
            .to_list()
        )

        return results

    def delete_version(
        self,
        version_id: int,
    ) -> None:
        """删除指定版本的全部向量。"""

        if version_id < 0:
            raise ValueError("version_id cannot be negative")

        if self.table_name not in self.db.table_names():
            return

        table = self.db.open_table(self.table_name)

        table.delete(f"version_id = {version_id}")

    def copy_version(
        self,
        source_version_id: int,
        target_version_id: int,
        allowed_revisions: dict[int, tuple[int, str, str]],
    ) -> dict[int, set[int]]:
        """
        复制源版本中符合目标版本快照的向量。

        allowed_revisions:
            {
                document_id: (revision_id, content_hash, pipeline_hash)
            }

        返回:
            {
                document_id: {chunk_index_1, chunk_index_2}
            }

        注意：
            1. 不重新生成 Embedding。
            2. 只复制 revision、content_hash、pipeline_hash
            完全匹配的文档向量。
            3. 目标版本使用新的 version_id 和向量记录 ID。
            4. 通过 add_records 保证重复写入时的应用层幂等。
        """

        # ==================================================
        # 1. 参数校验
        # ==================================================

        if source_version_id < 0 or target_version_id < 0:
            raise ValueError("version_id cannot be negative")

        if source_version_id == target_version_id:
            raise ValueError(
                "Source and target versions must differ"
            )

        # ==================================================
        # 2. 检查向量表是否存在
        # ==================================================

        if self.table_name not in self.db.table_names():
            return {}

        table = self.db.open_table(self.table_name)

        # ==================================================
        # 3. 读取 LanceDB 全部记录
        # ==================================================

        arrow_table = table.to_arrow()

        all_records = arrow_table.to_pylist()

        # ==================================================
        # 4. 筛选源版本的向量
        # ==================================================

        source_records = [
            record
            for record in all_records
            if record["version_id"] == source_version_id
        ]

        if not source_records:
            return {}

        # ==================================================
        # 5. 检查源版本中是否存在重复 chunk
        # ==================================================

        seen_chunks: dict[int, set[int]] = {}

        for record in source_records:
            document_id = record["document_id"]
            chunk_index = record["chunk_index"]

            document_chunks = seen_chunks.setdefault(
                document_id,
                set(),
            )

            if chunk_index in document_chunks:
                raise RuntimeError(
                    "Duplicate chunk detected in source version: "
                    f"document_id={document_id}, "
                    f"chunk_index={chunk_index}"
                )

            document_chunks.add(chunk_index)

        # ==================================================
        # 6. 根据目标版本快照筛选允许复制的记录
        # ==================================================

        records_to_copy = []

        copied_by_document: dict[int, set[int]] = {}

        for record in source_records:
            document_id = record["document_id"]

            # 目标版本快照中没有这篇文档，不复制
            if document_id not in allowed_revisions:
                continue

            expected_revision_id, expected_content_hash, expected_pipeline_hash = (
                allowed_revisions[document_id]
            )

            # 三个字段必须全部匹配
            if (
                record["revision_id"] != expected_revision_id
                or record["content_hash"] != expected_content_hash
                or record["pipeline_hash"] != expected_pipeline_hash
            ):
                continue

            # ==================================================
            # 7. 构建目标版本的新向量记录
            # ==================================================

            new_record = VectorRecord(
                id=(
                    f"{target_version_id}:"
                    f"{document_id}:"
                    f"{expected_revision_id}:"
                    f"{record['chunk_index']}"
                ),
                version_id=target_version_id,
                document_id=document_id,
                revision_id=expected_revision_id,
                chunk_index=record["chunk_index"],
                content_hash=expected_content_hash,
                pipeline_hash=expected_pipeline_hash,
                text=record["text"],
                vector=record["vector"],
            )

            records_to_copy.append(new_record)

            copied_by_document.setdefault(
                document_id,
                set(),
            ).add(record["chunk_index"])

        # ==================================================
        # 8. 写入目标版本
        # ==================================================

        if records_to_copy:
            self.add_records(records_to_copy)

        # ==================================================
        # 9. 返回每篇文档已经复制的 chunk
        # ==================================================

        return copied_by_document