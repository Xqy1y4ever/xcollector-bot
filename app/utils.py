"""通用工具：ID 生成、时间换算、文本截断。

刻意保持极小 —— bot 不该长成一个"什么都有"的仓库。
"""

from __future__ import annotations

import secrets
import time


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id() -> str:
    """时间有序的短 ID（前 13 位是毫秒时间戳），用于 OneBot 的 echo 匹配。

    用自增计数器也能做 echo，但进程重启后会从 0 重来，
    万一 NapCat 那边残留了旧响应就会串台；时间戳前缀能天然避开这一点。
    """
    return f"{now_ms():013d}{secrets.token_hex(4)}"


def truncate(text: str, limit: int, suffix: str = "…") -> str:
    """超长截断。返回长度最多 limit + len(suffix)。"""
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + suffix
