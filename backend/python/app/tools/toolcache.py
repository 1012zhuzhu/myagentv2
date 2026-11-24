from copy import deepcopy
import hashlib
import json
import time
from collections import OrderedDict
from typing import Any, Protocol

class CacheBackend(Protocol):
    @staticmethod
    def make_key(
        tool_name: str,
        args: dict[str, Any]
    ) -> str:
        ...

    async def get(self, key: str) -> tuple[bool, Any]:
        ...

    async def set(
        self,
        key: str,
        value: Any,
        ttl: int
    ) -> None:
        ...

class ToolCache:
    def __init__(self, max_entries: int = 1000):
        if max_entries <= 0:
            raise ValueError("max_entries must be greater than 0")

        self.max_entries = max_entries

        self._data: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    @staticmethod
    def make_key(tool_name: str, args: dict[str,Any]) -> str:
        raw = json.dumps(
            {
                "tool_name": tool_name,
                "args": args
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",",":")
        )

        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    async def get(self, key: str) -> tuple[bool, Any]:
        entry = self._data.get(key)

        if entry is None:
            return False, None

        expires_at, value = entry

        if time.monotonic() >= expires_at:
            del self._data[key]
            return False, None

        self._data.move_to_end(key)

        return True, deepcopy(value)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        if ttl <= 0:
            return

        self._data.pop(key, None)

        now = time.monotonic()
        expired_keys = [
            cache_key
            for cache_key, (expires_at, _) in self._data.items()
            if now >= expires_at
        ]

        for cache_key in expired_keys:
            del self._data[cache_key]

        if len(self._data) >= self.max_entries:
            self._data.popitem(last=False)

        self._data[key] = (
            now + ttl,
            deepcopy(value)
        )

    def cleanup(self) -> int:
        now = time.monotonic()

        expired_keys = [
            key
            for key, (expires_at, _) in self._data.items()
            if now >= expires_at
        ]

        for key in expired_keys:
            del self._data[key]

        return len(expired_keys)

    def clear(self) -> None:
        self._data.clear()