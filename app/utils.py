"""通用工具：ID 生成、时间换算、文本截断。

刻意保持极小 —— bot 不该长成一个"什么都有"的仓库。
时间换算这几个函数是从后端 `utils.py` 原样搬过来的：抽取、digest、消息日志
都要按同一套时区规则渲染时间，实现只能有一份。
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime, timezone

from .config import get_settings


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


# ---------------------------------------------------------------------------
# 时间（时区统一取 DIGEST_TZ，默认 Asia/Shanghai）
# ---------------------------------------------------------------------------


def local_day(ts_ms: int | None = None) -> str:
    """返回配置时区下的 YYYY-MM-DD。"""
    ts = now_ms() if ts_ms is None else ts_ms
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(get_settings().tz)
    return dt.strftime("%Y-%m-%d")


def to_local(ts_ms: int | None) -> datetime | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(get_settings().tz)


def iso_local(ts_ms: int | None) -> str | None:
    dt = to_local(ts_ms)
    return dt.isoformat() if dt else None


def parse_iso_to_ms(value: str | None) -> int | None:
    """把 ISO8601（带或不带时区）转成毫秒时间戳。

    不带时区时按配置时区解释 —— 这一条很关键：
    LLM 经常返回 "2025-09-12T23:59:00" 而漏掉偏移量，
    如果按 UTC 解释就会整体偏 8 小时。
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=get_settings().tz)
    return int(dt.timestamp() * 1000)
