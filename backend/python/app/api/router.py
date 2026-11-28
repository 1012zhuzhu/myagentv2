
import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request
from openai import (
    AuthenticationError,
    APIError,
    APITimeoutError,
)

from app.schemas.agentconfig import (
    AgentExecuteRequest,
    AgentExecuteResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


async def wait_for_disconnect(http_request: Request) -> None:
    """
    持续监听客户端是否断开连接。

    检测到客户端断开时正常返回。
    """
    while True:
        if await http_request.is_disconnected():
            return

        await asyncio.sleep(0.1)


@router.post("/internal/v1/agent/execute")
async def execute(
    request: AgentExecuteRequest,
    http_request: Request,
) -> AgentExecuteResponse:

    agent_service = http_request.app.state.agent_service

    run_id = request.run_id
    node_id = request.node_id

    def error_detail(code: str, message: str):
        return {
            "code": code,
            "message": message,
            "run_id": run_id,
            "node_id": node_id,
        }

    execution_task = asyncio.create_task(
        agent_service.execute(request),
        name=f"agent-execution-{run_id}-{node_id}",
    )

    disconnect_task = asyncio.create_task(
        wait_for_disconnect(http_request),
        name=f"disconnect-watcher-{run_id}-{node_id}",
    )

    try:
        logger.info(
            "Agent execution started run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        done, _ = await asyncio.wait(
            {execution_task, disconnect_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Agent 执行先完成，正常返回结果。
        # 如果两个任务同时完成，优先处理 Agent 的执行结果。
        if execution_task in done:
            result = await execution_task

            logger.info(
                "Agent execution succeeded run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            return result

        # 监听任务先完成，读取它的结果。
        # 正常返回代表检测到了客户端断开；
        # 如果监听任务异常，这里会直接抛出异常。
        await disconnect_task

        logger.info(
            "Client disconnected, cancelling agent execution "
            "run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        # 取消 Agent 执行任务。
        execution_task.cancel()

        # 等待 Agent 响应取消，避免留下未完成的任务。
        await asyncio.gather(
            execution_task,
            return_exceptions=True,
        )

        raise asyncio.CancelledError(
            f"Client disconnected: run_id={run_id}, node_id={node_id}"
        )

    except asyncio.CancelledError:
        logger.info(
            "Agent execution cancelled run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        # 路由自身被取消时，将取消继续传递给 Agent。
        if not execution_task.done():
            execution_task.cancel()

        raise

    except AuthenticationError:
        logger.exception(
            "Model authentication failed run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        raise HTTPException(
            status_code=502,
            detail=error_detail(
                "MODEL_AUTH_FAILED",
                "DeepSeek API Key 无效，请检查配置。",
            ),
        )

    except APITimeoutError:
        logger.exception(
            "Model request timed out run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        raise HTTPException(
            status_code=504,
            detail=error_detail(
                "MODEL_TIMEOUT",
                "模型请求超时，请稍后重试。",
            ),
        )

    except APIError:
        logger.exception(
            "Model provider API error run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        raise HTTPException(
            status_code=502,
            detail=error_detail(
                "MODEL_PROVIDER_ERROR",
                "模型服务调用失败，请检查模型服务配置或稍后重试。",
            ),
        )

    except Exception:
        logger.exception(
            "Agent execution failed run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        raise HTTPException(
            status_code=500,
            detail=error_detail(
                "AGENT_EXECUTION_FAILED",
                "Agent 执行过程中发生内部错误。",
            ),
        )

    finally:
        # 无论正常完成、异常退出还是路由被取消，
        # 都清理尚未结束的 Agent 执行任务。
        if not execution_task.done():
            execution_task.cancel()

        await asyncio.gather(
            execution_task,
            return_exceptions=True,
        )

        # 清理客户端断开监听任务。
        if not disconnect_task.done():
            disconnect_task.cancel()

        await asyncio.gather(
            disconnect_task,
            return_exceptions=True,
        )
