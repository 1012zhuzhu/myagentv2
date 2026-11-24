
import json
from typing import Any

from redis.asyncio import Redis

from app.tools.toolcache import ToolCache


class RedisToolCache:
    def __init__(self, redis: Redis):
        self.redis = redis

    @staticmethod
    def make_key(tool_name: str, args: dict[str, Any]) -> str:
        return ToolCache.make_key(tool_name, args)

    async def get(self, key: str) -> tuple[bool, Any]:
        raw = await self.redis.get(key)

        if raw is None:
            return False, None

        value = json.loads(raw)
        return True, value

    async def set(self, key: str, value: Any, ttl: int) -> None:
        if ttl <= 0:
            return

        serialized = json.dumps(value, ensure_ascii=False)

        await self.redis.set(
            key,
            serialized,
            ex=ttl
        )
