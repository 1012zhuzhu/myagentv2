from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from app.api.agent import router as agent_router
from app.infrastructure.database import (
    init_db,
    recover_interrupted_tasks,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "agent.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    init_db(str(DB_PATH))

    recovered_tasks = recover_interrupted_tasks(str(DB_PATH))

    print(f"Recovered interrupted index tasks: {recovered_tasks}")

    yield


app = FastAPI(
    title="MY agent Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(agent_router)


@app.get("/health")
def health():
    return {"status": "ok"}