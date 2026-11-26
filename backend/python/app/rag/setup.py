from app.rag.embedder import Embedder
from app.rag.index_builder import IndexBuilder
from app.rag.index_service import IndexService
from app.rag.pipeline import PipelineConfig
from app.rag.retriever import Retriever
from app.rag.vector_store import VectorStore


def create_rag_system(
    db_path: str,
    vector_db_path: str,
    embedding_model: str,
    embedding_model_path: str,
):
    # 1. 创建 Pipeline 配置
    config = PipelineConfig(
        embedding_model=embedding_model,
        embedding_model_path=embedding_model_path,
    )

    # 2. 创建 Embedding 模型
    embedder = Embedder(config)

    # 3. 创建向量数据库
    vector_store = VectorStore(
        db_url=vector_db_path,
    )

    # 4. 创建检索器
    retriever = Retriever(
        db_path=db_path,
        embedder=embedder,
        vector_store=vector_store,
    )

    # 5. 创建索引构建器
    index_builder = IndexBuilder(
        vector_store=vector_store,
        embedder=embedder,
        config=config,
    )

    # 6. 创建索引服务
    index_service = IndexService(
        db_path=db_path,
        index_builder=index_builder,
        vector_store=vector_store,
    )

    return retriever, index_service