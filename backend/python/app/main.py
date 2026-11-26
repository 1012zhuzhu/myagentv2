
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

from fastapi import FastAPI
from redis.asyncio import Redis

from app.api.agent import router as agent_router
from app.infrastructure.database import (
    init_db,
    recover_interrupted_tasks,
)
from app.agent.runtime_impl import LangGraphRuntime
from app.rag.setup import create_rag_system
from app.service.agent_service import AgentService


BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "data" / "agent.db"
VECTOR_DB_PATH = BASE_DIR / "data" / "lancedb"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 初始化 SQLite
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    init_db(str(DB_PATH))

    recovered_tasks = recover_interrupted_tasks(str(DB_PATH))

    logging.info(
        "Recovered interrupted index tasks: %s",
        recovered_tasks,
    )

    # 2. 创建 Redis 客户端
    redis_client = Redis.from_url(
        "redis://localhost:6379/0",
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
        health_check_interval=30,
    )

    try:
        # 3. 初始化 RAG 系统
        retriever, index_service = create_rag_system(
            db_path=str(DB_PATH),
            vector_db_path=str(VECTOR_DB_PATH),
            embedding_model=os.environ["EMBEDDING_MODEL"],
            embedding_model_path=os.environ["EMBEDDING_MODEL_PATH"],
        )

        # 4. 创建 Runtime 和 Agent Service
        runtime = LangGraphRuntime(
            redis_client=redis_client,
            retriever=retriever,
        )

        agent_service = AgentService(runtime=runtime)

        # 5. 将 Service 放入 FastAPI 应用状态
        app.state.agent_service = agent_service
        app.state.index_service = index_service
        app.state.redis_client = redis_client
        app.state.retriever = retriever

        logging.info("RAG system initialized successfully")

        yield

    finally:
        # 6. 应用关闭时释放 Redis 连接
        await redis_client.aclose()


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s %(levelname)s "
        "[%(name)s] %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = FastAPI(
    title="MY agent Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(agent_router)


@app.get("/health")
def health():
    return {"status": "ok"}
