"""每条消息的处理轨迹：**先留痕，再干活**，最后一行汇总。

排障时要能直接在日志里看到"这条消息进来之后到底发生了什么"，所以一条消息会产生：

    阶段=收到   | 群=… | 发送者=… | msg_id=… | 发送时间=… | 原文=…
    阶段=归一化 | msg_id=… | 附件=2 | 耗时=12ms
    阶段=入库   | msg_id=… | raw_id=… | 幂等=新 | 耗时=48ms
    阶段=附件   | msg_id=… | 已存=2 失败=0 | 耗时=340ms
    阶段=抽取   | msg_id=… | 抽取器=llm | 模型=… | tokens=812 | 耗时=1203ms
    结果=extracted | 群=… | 标题=… | 截止=… | …（最后一行的汇总）

`阶段=` 和 `结果=` 都是固定前缀，`grep '阶段='` 看轨迹、`grep '结果='` 看结论。

**为什么"收到"必须排在最前面**：归一化要查群名（一次网络往返），附件要下载上传，
抽取要调模型 —— 这些加起来可能几十秒。如果只在处理完之后打一行，一条卡住的消息
在日志里就完全看不到，"到底收到没有"只能靠猜；而这条链路最怕的就是静默。

日志级别约定（`LOG_LEVEL` 默认 INFO）：
  - INFO    ：正常路径 —— 收到（群在白名单内）/ 各阶段 / extracted / noise /
              skipped_whitelist
  - WARNING ：值得注意 —— unparsed（本该抽出却没抽出）/ degraded / error
  - DEBUG   ：量大且重复 —— 收到（群**不**在白名单内）/ group_filtered / duplicate
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

# OneBot 消息段类型 → 日志里的一小段可读文本。
# 只映射"看了就知道是什么"的几种，其余按 [类型] 兜底。
_SEGMENT_LABELS = {
    "image": "[图片]",
    "face": "[表情]",
    "record": "[语音]",
    "video": "[视频]",
    "file": "[文件]",
    "forward": "[合并转发]",
    "json": "[卡片]",
    "xml": "[卡片]",
    "reply": "[回复]",
    "poke": "[戳一戳]",
}


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


# ---------------------------------------------------------------------------
# 处理轨迹：收到 + 各阶段
# ---------------------------------------------------------------------------


def _fmt_event_ts(seconds: Any) -> str:
    """OneBot 事件的 `time` 是**秒**，这里统一成和别处一样的本地时间显示。"""
    try:
        dt = to_local(int(seconds) * 1000)
    except (TypeError, ValueError):
        dt = None
    return dt.strftime("%m-%d %H:%M:%S") if dt else "?"


def event_preview(event: Mapping[str, Any]) -> str:
    """从**原始 OneBot 事件**里抠出一段可读正文。

    不能等归一化之后再打：归一化要先查群名（一次网络往返），而"收到"这行的
    意义恰恰是**在处理之前就留下痕迹**。所以这里只做最轻的解析。
    """
    msg = event.get("message")
    if isinstance(msg, str):
        text = msg  # CQ 码字符串形式
    elif isinstance(msg, list):
        bits: list[str] = []
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            kind = str(seg.get("type") or "")
            data = seg.get("data") or {}
            if kind == "text":
                bits.append(str(data.get("text") or ""))
            elif kind == "at":
                qq = str(data.get("qq"))
                bits.append("@全体成员" if qq == "all" else f"@{qq}")
            else:
                bits.append(_SEGMENT_LABELS.get(kind, f"[{kind or '未知'}]"))
        text = "".join(bits)
    else:
        text = str(event.get("raw_message") or "")
    return one_line(text) or "(无正文)"


def log_received(
    event: Mapping[str, Any],
    *,
    level: int = logging.INFO,
    group_name: str | None = None,
) -> str:
    """**收到消息的第一行日志**，在处理之前打。

    群不在白名单时调用方会传 DEBUG —— 那类消息量大且重复，INFO 会把日志刷爆，
    而且它们的结局（group_filtered）本来也只值 DEBUG。两边的级别保持一致，
    才不会出现"只有收到、没有下文"的困惑行。

    `group_name` 传得进来就带上（调用方手里的群名缓存，**不查网络**）。
    查不到就只显示群号 —— 这行的意义是快，不是全。
    """
    sender = event.get("sender") or {}
    who = sender.get("card") or sender.get("nickname") or event.get("user_id")
    group_id = event.get("group_id")
    group = f"{group_name}({group_id})" if group_name else str(group_id)
    line = " | ".join(
        [
            "阶段=收到",
            f"群={group}",
            f"发送者={who}({event.get('user_id')})",
            f"msg_id={event.get('message_id')}",
            f"发送时间={_fmt_event_ts(event.get('time'))}",
            f"原文={event_preview(event)}",
        ]
    )
    logger.log(level, line)
    return line


def log_stage(
    stage: str,
    message: Mapping[str, Any] | None = None,
    *,
    level: int = logging.INFO,
    **extra: Any,
) -> str:
    """处理过程中的一步。

    格式与汇总行一致（`键=值 | 键=值`），开头固定是 `阶段=`，
    所以 `grep '阶段='` 能看到一条消息的完整轨迹。
    """
    parts = [f"阶段={stage}"]
    if message is not None:
        parts.append(f"msg_id={message.get('message_id')}")
    for key, value in extra.items():
        if value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    line = " | ".join(parts)
    logger.log(level, line)
    return line


def elapsed_ms(started: float) -> str:
    """把 `time.monotonic()` 的起点换算成 `123ms` / `1.2s`。

    阶段日志里带耗时的意义：出问题时一眼能看出慢在哪一步 ——
    是查群名卡住了、附件下载卡住了，还是模型调用卡住了。
    """
    import time

    ms = (time.monotonic() - started) * 1000.0
    return f"{ms:.0f}ms" if ms < 1000 else f"{ms / 1000:.1f}s"
