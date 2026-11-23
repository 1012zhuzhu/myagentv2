from app.rag.document_store import get_version_documents
from app.rag.chunker import split_text
from app.rag.embedder import Embedder
from app.rag.models import (
    IndexBuildResult,
    VectorRecord,
    build_vector_id,
)
from app.rag.vector_store import VectorStore
from app.rag.pipeline import PipelineConfig


class IndexBuilder:
    def __init__(
        self,
        vector_store: VectorStore,
        embedder: Embedder,
        config: PipelineConfig,
    ):
        self.vector_store = vector_store
        self.embedder = embedder
        self.config = config

        # 确保 Builder 和 Embedder 使用完全相同的配置
        if (
            self.embedder.config.calculate_hash()
            != self.config.calculate_hash()
        ):
            raise ValueError(
                "IndexBuilder and Embedder must use the same PipelineConfig"
            )

    def build_version(
        self,
        db_path: str,
        target_version_id: int,
        source_version_id: int | None = None,
    ) -> IndexBuildResult:

        # 1. 获取目标版本固定的文档快照
        documents = get_version_documents(
            db_path,
            target_version_id,
        )

        # 2. 校验数据库记录的 pipeline_hash
        expected_pipeline_hash = self.config.calculate_hash()

        for document in documents:
            if document.pipeline_hash != expected_pipeline_hash:
                raise ValueError(
                    f"Pipeline config mismatch for document "
                    f"{document.document_id}: "
                    f"expected {expected_pipeline_hash}, "
                    f"got {document.pipeline_hash}"
                )

        # 3. 构建允许复用的文档修订映射
        allowed_revisions = {
            document.document_id: (
                document.revision_id,
                document.content_hash,
                document.pipeline_hash,
            )
            for document in documents
        }

        # 4. 尝试复用源版本中已有的向量
        copied_by_document: dict[int, set[int]] = {}

        if source_version_id is not None:
            copied_by_document = self.vector_store.copy_version(
                source_version_id=source_version_id,
                target_version_id=target_version_id,
                allowed_revisions=allowed_revisions,
            )

        new_records: list[VectorRecord] = []

        total_chunk_count = 0
        copied_vector_count = 0

        # 5. 遍历目标版本的所有文档
        for document in documents:

            # Chunk 参数来自同一份 PipelineConfig
            chunks = split_text(
                document.content,
                chunk_size=self.config.chunk_size,
                overlap=self.config.overlap,
            )

            total_chunk_count += len(chunks)

            expected_chunks = {
                chunk.index
                for chunk in chunks
            }

            copied_chunks = copied_by_document.get(
                document.document_id,
                set(),
            )

            # 6. 检查是否完整复用
            if copied_chunks == expected_chunks:
                copied_vector_count += len(copied_chunks)
                continue

            # 部分复制属于异常状态，不能静默跳过
            if copied_chunks:
                raise RuntimeError(
                    f"Incomplete copied vectors for document "
                    f"{document.document_id}: "
                    f"expected chunks {expected_chunks}, "
                    f"copied chunks {copied_chunks}"
                )

            # 空文档不需要计算向量
            if not chunks:
                continue

            # 7. 计算 Embedding
            texts = [
                chunk.content
                for chunk in chunks
            ]

            embeddings = self.embedder.embed_texts(texts)

            if len(embeddings) != len(chunks):
                raise RuntimeError(
                    f"Embedding count mismatch for document "
                    f"{document.document_id}: "
                    f"expected {len(chunks)}, "
                    f"got {len(embeddings)}"
                )

            # 8. 组合 Chunk 和 Embedding
            for chunk, embedding in zip(chunks, embeddings):

                record = VectorRecord(
                    id=build_vector_id(
                        version_id=target_version_id,
                        document_id=document.document_id,
                        revision_id=document.revision_id,
                        chunk_index=chunk.index,
                    ),
                    version_id=target_version_id,
                    document_id=document.document_id,
                    revision_id=document.revision_id,
                    chunk_index=chunk.index,
                    content_hash=document.content_hash,
                    pipeline_hash=document.pipeline_hash,
                    text=chunk.content,
                    vector=list(embedding),
                )

                new_records.append(record)

        # 9. 批量写入新向量
        self.vector_store.add_records(new_records)

        # 10. 汇总构建结果
        new_vector_count = len(new_records)

        return IndexBuildResult(
            version_id=target_version_id,
            document_count=len(documents),
            chunk_count=total_chunk_count,
            vector_count=copied_vector_count + new_vector_count,
        )