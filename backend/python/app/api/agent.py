
import logging

from fastapi import APIRouter, HTTPException, Request
from openai import AuthenticationError, APIError

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

    try:
        return await agent_service.execute(request)

    except AuthenticationError:
        logger.exception("DeepSeek authentication failed")

        raise HTTPException(
            status_code=502,
            detail={
                "code": "MODEL_AUTH_FAILED",
                "message": "DeepSeek API Key 无效，请检查配置。",
            },
        )

    except APIError:
        logger.exception("Model provider API error")

        raise HTTPException(
            status_code=502,
            detail={
                "code": "MODEL_PROVIDER_ERROR",
                "message": "模型服务调用失败，请检查模型服务配置或稍后重试。",
            },
        )

    except Exception:
        logger.exception("Agent execution failed")

        raise HTTPException(
            status_code=500,
            detail={
                "code": "AGENT_EXECUTION_FAILED",
                "message": "Agent 执行过程中发生内部错误。",
            },
        )
