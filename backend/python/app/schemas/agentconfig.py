from typing import Any

from pydantic import BaseModel, Field


class AgentConfig(BaseModel):
    system_prompt: str
    model: str
    allowed_tools: list[str] = Field(default_factory=list)

class AgentExecuteRequest(BaseModel):
    run_id: str
    node_id: str
    input: Any
    config: AgentConfig

class AgentExecuteResponse(BaseModel):
    output: Any