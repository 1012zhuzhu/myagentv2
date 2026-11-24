
import asyncio
import time
from typing import Any

from app.tools.toolcache import ToolCache
from app.tools.rediscache import RedisToolCache


class ResilientToolCache:
    def __init__(
        self,
        redis_cache: RedisToolCache,
        fallback_cache: ToolCache,
        recovery_interval: int = 5,
    ):
        if recovery_interval <= 0:
            raise ValueError("recovery_interval must be greater than 0")

        self.redis_cache = redis_cache
        self.fallback_cache = fallback_cache
        self.recovery_interval = recovery_interval

        # 是否正在使用本地降级缓存
        self._fallback_mode = False

        # 上一次 Redis 探测时间
        self._last_probe = 0.0

        # 避免多个请求同时探测 Redis
        self._recovery_lock = asyncio.Lock()

    @staticmethod
    def make_key(
        tool_name: str,
        args: dict[str, Any],
    ) -> str:
        return ToolCache.make_key(tool_name, args)

    async def _try_recover(self) -> bool:
        """
        尝试恢复 Redis。

        返回 True：仍然处于本地降级模式。
        返回 False：Redis 已恢复，可以使用 Redis。
        """
        if not self._fallback_mode:
            return False

        now = time.monotonic()

        # 未到探测时间，继续使用本地缓存
        if now - self._last_probe < self.recovery_interval:
            return True

        async with self._recovery_lock:
            # 获取锁后再次检查，避免重复探测
            if not self._fallback_mode:
                return False

            now = time.monotonic()

            if now - self._last_probe < self.recovery_interval:
                return True

            self._last_probe = now

            try:
                await self.redis_cache.redis.ping()
            except Exception:
                return True

            # Redis 恢复：清空降级期间产生的本地缓存
            self.fallback_cache.clear()

            self._fallback_mode = False

            return False

    def _switch_to_fallback(self) -> None:
        self._fallback_mode = True
        self._last_probe = time.monotonic()

    async def get(self, key: str) -> tuple[bool, Any]:
        # 已经降级，先检查是否可以恢复
        if await self._try_recover():
            return await self.fallback_cache.get(key)

        try:
            return await self.redis_cache.get(key)
        except Exception:
            self._switch_to_fallback()
            return await self.fallback_cache.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        ttl: int,
    ) -> None:
        # 已经降级，先检查是否可以恢复
        if await self._try_recover():
            await self.fallback_cache.set(key, value, ttl)
            return

        try:
            await self.redis_cache.set(key, value, ttl)
        except Exception:
            self._switch_to_fallback()
            await self.fallback_cache.set(key, value, ttl)

    @property
    def is_fallback(self) -> bool:
        return self._fallback_mode