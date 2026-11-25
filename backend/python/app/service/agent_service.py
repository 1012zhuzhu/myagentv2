
import logging

from app.schemas.agentconfig import (
    AgentExecuteRequest,
    AgentExecuteResponse,
)
from app.agent.runtime import AgentRuntime

logger = logging.getLogger(__name__)


class AgentService:
    def __init__(self, runtime: AgentRuntime):
        self.runtime = runtime

    async def execute(
        self,
        request: AgentExecuteRequest,
    ) -> AgentExecuteResponse:

        run_id = request.run_id
        node_id = request.node_id

        logger.info(
            "Agent execution started run_id=%s node_id=%s",
            run_id,
            node_id,
        )

        try:
            output = await self.runtime.execute(request)

            logger.info(
                "Agent execution succeeded run_id=%s node_id=%s",
                run_id,
                node_id,
            )

            return AgentExecuteResponse(
                output=output
            )

        except Exception as exc:
            logger.error(
                "Agent execution failed run_id=%s node_id=%s error=%s",
                run_id,
                node_id,
                exc,
            )
            raise
