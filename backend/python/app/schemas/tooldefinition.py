from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from pydantic import BaseModel

class ToolDefinition(BaseModel):
    name: str
    description: str
    parameters: dict[str,Any]

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    retry_delay: float = 1.0

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        if self.retry_delay < 0:
            raise ValueError("retry_delay must be greater than or equal to 0")

@dataclass
class Tool:
    name: str
    description: str
    parameters: dict[str, Any]
    execute: Callable[[dict[str,Any]],Awaitable[Any]]

    cacheable: bool = False
    cache_ttl: int = 60

    def _post_init_(self):
        if self.cache_ttl <= 0:
            raise ValueError("cache_ttl must be greater than 0")

    retry_policy: RetryPolicy = field(
        default_factory=RetryPolicy
    )