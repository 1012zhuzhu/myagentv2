from fastapi import APIRouter, Request

from app.schemas.agentconfig import (
    AgentExecuteRequest,
    AgentExecuteResponse,
)

router = APIRouter()


@router.post("/internal/v1/agent/execute")
async def execute(
    request: AgentExecuteRequest,
    http_request: Request,
) -> AgentExecuteResponse:

    agent_service = http_request.app.state.agent_service

    return await agent_service.execute(request)