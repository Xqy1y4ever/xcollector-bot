"""OneBot 11 消息段解析。

把 NapCat 发来的 message（数组或 CQ 码字符串）归一化为：
  - 可读文本（图片/表情等以占位符呈现，避免文本为空导致 LLM 无从下手）
  - 附件列表（图片/文件/语音等，URL 会随时间失效，调用方必须落地保存）
  - 合并转发的 id 列表（必须递归展开，否则内容为空 —— 这是接入期最常见的坑）
  - 是否 @全体成员、被 @ 的人、回复的目标
"""

from __future__ import annotations

import html
import re
from dataclasses import asdict, dataclass, field

CQ_PATTERN = re.compile(r"\[CQ:([a-zA-Z_]+)((?:,[^\]]*)?)\]")

ATTACHMENT_TYPES = {"image", "file", "video", "record", "mface"}


@dataclass
class Attachment:
    type: str
    url: str | None = None
    file: str | None = None
    name: str | None = None
    size: int | None = None
    local_path: str | None = None
    extracted_text: str | None = None
    download_error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ParsedMessage:
    text: str = ""
    at_all: bool = False
    mentions: list[str] = field(default_factory=list)
    attachments: list[Attachment] = field(default_factory=list)
    forwards: list[str] = field(default_factory=list)
    reply_id: str | None = None
    # 原始消息里出现过的附件 URL，用于去重下载
    has_media: bool = False


def _cq_params(raw: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for part in raw.lstrip(",").split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        params[k.strip()] = html.unescape(v.strip())
    return params


def parse_message(message) -> ParsedMessage:
    """解析 OneBot 消息体。message 可以是 list[dict] 或 CQ 码 str。"""
    segments: list[dict] = []
    if isinstance(message, str):
        segments = _parse_cq_string(message)
    elif isinstance(message, list):
        for seg in message:
            if isinstance(seg, dict):
                segments.append(
                    {
                        "type": seg.get("type", "text"),
                        "data": seg.get("data") or {},
                    }
                )
    return _render(segments)


def _parse_cq_string(text: str) -> list[dict]:
    segments: list[dict] = []
    pos = 0
    for m in CQ_PATTERN.finditer(text):
        if m.start() > pos:
            plain = text[pos : m.start()]
            if plain:
                segments.append({"type": "text", "data": {"text": plain}})
        segments.append({"type": m.group(1), "data": _cq_params(m.group(2))})
        pos = m.end()
    if pos < len(text):
        segments.append({"type": "text", "data": {"text": text[pos:]}})
    return segments


def _render(segments: list[dict]) -> ParsedMessage:
    out = ParsedMessage()
    parts: list[str] = []
    for seg in segments:
        stype = seg.get("type", "text")
        data = seg.get("data") or {}

        if stype == "text":
            parts.append(str(data.get("text", "")))

        elif stype == "at":
            qq = str(data.get("qq", ""))
            if qq == "all":
                out.at_all = True
                parts.append("@全体成员")
            else:
                out.mentions.append(qq)
                parts.append(f"@{data.get('name') or qq}")

        elif stype in ATTACHMENT_TYPES:
            att = Attachment(
                type=stype,
                url=data.get("url") or data.get("file_uri") or None,
                file=data.get("file") or None,
                name=data.get("name") or data.get("file") or None,
                size=int(data["size"]) if str(data.get("size", "")).isdigit() else None,
            )
            out.attachments.append(att)
            out.has_media = True
            parts.append(_placeholder(stype, att))

        elif stype == "forward":
            fid = str(data.get("id", ""))
            if fid:
                out.forwards.append(fid)
            parts.append("[合并转发]")

        elif stype == "reply":
            out.reply_id = str(data.get("id", "")) or None

        elif stype == "face":
            parts.append("[表情]")

        elif stype == "json":
            # 分享卡片 / 结构化卡片，正文常藏在 data 里
            parts.append(f"[卡片] {str(data.get('data', ''))[:200]}")

        elif stype == "xml":
            parts.append("[XML卡片]")

        else:
            parts.append(f"[{stype}]")

    out.text = "".join(parts).strip()
    return out


def _placeholder(stype: str, att: Attachment) -> str:
    if stype == "image":
        return "[图片]"
    if stype == "file":
        return f"[文件: {att.name or '未命名'}]"
    if stype == "record":
        return "[语音]"
    if stype == "video":
        return "[视频]"
    return f"[{stype}]"
