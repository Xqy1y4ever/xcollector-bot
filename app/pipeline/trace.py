"""每条消息一行结构化日志。

系统的第一条原则是「绝不静默丢弃」，但光有数据库和盲区计数还不够 ——
排障时要能直接在日志里看到"这条消息进来之后到底发生了什么"。

所以每条被处理的消息都**恰好产生一行** key=value 记录，便于 grep：

    结果=extracted | 群=NOVA官方通知群(123456) | 发送者=李老师 | msg_id=12345 |
    发送时间=09-16 19:40:12 | 标题=提交军训心得 | 截止=09-23 23:59(下周三前) |
    置信度=0.7 | 抽取器=llm | 模型=deepseek/deepseek-chat | 附件=1 | 原文=大家下周三前…

日志级别约定（`LOG_LEVEL` 默认 INFO）：
  - INFO    ：正常路径 —— extracted / noise / skipped_whitelist
  - WARNING ：值得注意 —— unparsed（本该抽出却没抽出）/ degraded / error
  - DEBUG   ：量大且重复 —— group_filtered（群白名单外）/ duplicate（重复推送）
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from ..config import get_settings
from ..utils import to_local

logger = logging.getLogger("xcollector.message")

# 这些结果值得用 WARNING，因为它们是真正的盲区
_WARN_OUTCOMES = {"unparsed", "degraded", "error"}

# 这些结果只值 DEBUG，否则会淹没日志
_DEBUG_OUTCOMES = {"group_filtered", "duplicate"}


def one_line(text: Any, limit: int | None = None) -> str:
    """压成单行并截断 —— 一条消息一行，换行会破坏 grep。"""
    if text is None:
        return ""
    if limit is None:
        limit = get_settings().log_preview_chars
    flat = " ".join(str(text).split())
    if not flat:
        return ""
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _fmt_ts(ts: Any) -> str:
    try:
        dt = to_local(int(ts)) if ts else None
    except (TypeError, ValueError):
        dt = None
    return dt.strftime("%m-%d %H:%M:%S") if dt else "?"


def fmt_due(due_at: Any, due_text: Any = None) -> str | None:
    """把截止时间渲染成 `09-23 23:59(下周三前)`，没有就返回 None。"""
    dt = None
    try:
        dt = to_local(int(due_at)) if due_at else None
    except (TypeError, ValueError):
        dt = None

    if dt is None:
        return f"原文「{due_text}」" if due_text else None

    stamp = dt.strftime("%m-%d %H:%M")
    return f"{stamp}({due_text})" if due_text else stamp


def raw_log_line(raw: Mapping[str, Any], outcome: str, **extra: Any) -> str:
    parts = [
        f"结果={outcome}",
        f"群={raw.get('group_name') or '未知'}({raw.get('group_id')})",
        f"发送者={raw.get('sender_name') or raw.get('sender_id')}",
        f"msg_id={raw.get('message_id')}",
        f"发送时间={_fmt_ts(raw.get('ts'))}",
    ]
    for key, value in extra.items():
        if value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    parts.append(f"原文={one_line(raw.get('content')) or '(空)'}")
    return " | ".join(parts)


def log_message(
    raw: Mapping[str, Any],
    outcome: str,
    *,
    level: int | None = None,
    **extra: Any,
) -> str:
    """记录一条消息的处理结果。返回日志行本身，便于测试。"""
    line = raw_log_line(raw, outcome, **extra)

    if level is None:
        if outcome in _WARN_OUTCOMES:
            level = logging.WARNING
        elif outcome in _DEBUG_OUTCOMES:
            level = logging.DEBUG
        else:
            level = logging.INFO

    logger.log(level, line)
    return line


def message_meta(message: Mapping[str, Any], *, content: str = "") -> dict[str, Any]:
    """把归一化消息整理成日志需要的字段。

    用于消息还没入库、或者压根不会入库的分支（群不在白名单）。
    归一化消息由 bot 提供，后端不认识 OneBot 协议。
    """
    message_id = message.get("message_id")
    ts = message.get("ts")
    attachments = message.get("attachments") or []
    meta: dict[str, Any] = {
        "group_id": message.get("group_id"),
        "group_name": message.get("group_name"),
        "sender_id": message.get("sender_id"),
        "sender_name": message.get("sender_name") or message.get("sender_id"),
        "message_id": message_id,
        "ts": int(ts) if ts else None,
        "content": content or (message.get("text") or ""),
    }
    if attachments:
        meta["附件"] = len(attachments)
    if message.get("at_all"):
        meta["@全体成员"] = "是"
    return meta


def describe_attachment_count(raw: Mapping[str, Any]) -> int:
    try:
        import json

        data = raw.get("attachments")
        if isinstance(data, str):
            data = json.loads(data or "[]")
        return len(data or [])
    except Exception:
        return 0
