
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

    try:
        logger.info(
            "Agent execution started run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        result = await agent_service.execute(request)

        logger.info(
            "Agent execution succeeded run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        return result

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
