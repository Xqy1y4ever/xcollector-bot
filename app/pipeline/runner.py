"""消息处理编排：一条 OneBot 群消息 → 后端里的一行 raw + 一条（或零条）通知。

数据流（契约 docs/api.md）：

    OneBot 事件
      → normalize（main.py，已有，不动）
      → 群白名单？不在 → DEBUG 日志，结束（纯本地判断，不会失败、不产生丢失窗口）
      → 【写前日志】POST /api/messages      ← 第一件落库的事，先保住原文
      → is_new=false → duplicate 日志，结束
      → 附件：下载字节 → POST /api/attachments → PATCH /api/messages/{id} 回填
      → POST /api/groups                    （拿回 previous_last_msg_ts 做缺口检测）
      → 发送者白名单？不在 → PATCH /api/messages/{id} state=skipped_whitelist
      → 抽取（按 EXTRACTOR：rule / llm / both）
      → POST /api/notifications  或  PATCH state=noise/unparsed/degraded
      → POST /api/stats
      → 一行消息日志（trace.py 的格式）

**为什么原文必须先落库**：QQ 群消息是唯一不可再生的资产（QQ 不会重发）。
附件下载、LLM 抽取都可能慢、可能崩、可能超时，把 POST /api/messages 放在它们
后面，就意味着"抽取途中进程被杀 = 这条消息永远不存在"。所以顺序是
**先写原文，再做一切重活**，附件是事后 PATCH 补齐的（契约第 1 节专门为此放开了
`attachments` 字段）。

**崩溃恢复**：raw_message 的 `state` 既是状态标记，也是恢复队列。
`resume_pending()` 在启动时和 OneBot 重连成功后把 `state=pending` 的消息捡回来
接着走 —— 这就是"绝不静默丢弃"在进程层面的落地。

三条原则在这里落地（和改造前一致）：
  - **绝不静默丢弃**：任何异常都会把 raw 标成 unparsed / degraded / error，原文始终在库里
  - **降级要留痕**：LLM 整体失败时用规则兜底，并记 degraded 统计
  - **分歧要暴露**：模型与规则、模型与模型之间的不一致，一律标 conflict 让人确认

抽取逻辑本身（timeparse / rule_extract / extract）是从后端原样搬过来的，
**没有重写** —— 只有"数据从哪来、写到哪去"换成了 HTTP。
"""

from __future__ import annotations

import base64
import logging
import mimetypes
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import httpx

from ..backend_client import BackendClient, BackendError
from ..config import Settings, get_settings
from ..llm.target import target_from_settings
from ..normalize import attachments_payload
from ..onebot.segments import parse_message
from ..utils import local_day, now_ms
from .extract import extract_with_llm
from .rule_extract import rule_extract
from .trace import fmt_due, log_message, message_meta

logger = logging.getLogger(__name__)

MANUAL_GROUP_NAME = "手动添加"
MANUAL_SENDER_FALLBACK = "unknown"

# 会被下载并上传给后端的附件类型。其它类型（表情、卡片）没有字节可下，
# 只留 source_url。图片是重点：官方通知经常把 DDL 写在图片里。
DOWNLOADABLE_TYPES = {"image", "file", "video", "record"}

# 下载 QQ CDN 的超时。比调后端的超时短：CDN 挂了不该拖住整条流水线。
MEDIA_TIMEOUT = 20.0

# 启动 / 重连后最多捡回多少条 pending 消息（契约 GET /api/messages 的 limit 上限是 1000）
RECOVERY_LIMIT = 200

_media_client: httpx.AsyncClient | None = None


# ---------------------------------------------------------------------------
# 内部规范化
# ---------------------------------------------------------------------------


def _canonical(msg: Any) -> dict:
    """归一化消息（dataclass 或 dict）→ 内部统一的 raw dict。

    统一成 dict 是为了让 trace / extract 这些从后端搬来的模块原样可用：
    它们读的是 `content` / `ts` / `message_id` 这几个键。
    """
    raw = msg.to_dict() if hasattr(msg, "to_dict") else dict(msg)
    raw["ts"] = int(raw.get("ts") or now_ms())
    raw["content"] = raw.get("content") or raw.get("text") or ""
    raw["message_id"] = str(raw.get("message_id") or "")
    raw["group_id"] = str(raw.get("group_id") or "")
    raw["sender_id"] = str(raw.get("sender_id") or "")
    return raw


def build_message_payload(raw: Mapping[str, Any], attachments: list[dict]) -> dict:
    """内部 raw dict → `POST /api/messages` 的请求体（契约第 1 节）。

    字段名对齐契约：正文叫 `content`（归一化消息里叫 `text`，不能直接透传，
    否则后端存下来的正文会是空的）。
    """
    return {
        "message_id": str(raw.get("message_id") or ""),
        "group_id": str(raw.get("group_id") or ""),
        "group_name": raw.get("group_name"),
        "sender_id": str(raw.get("sender_id") or ""),
        "sender_name": raw.get("sender_name") or raw.get("sender_id"),
        "ts": int(raw.get("ts") or now_ms()),
        "content": raw.get("content") or "",
        "attachments": attachments,
        "raw": dict(raw.get("raw") or {}),
    }


def doc_from_row(row: Mapping[str, Any]) -> dict:
    """后端返回的 raw_message 行 → 内部 raw dict（崩溃恢复用）。

    `at_all` 这几个字段不在 raw_message 的列里，但 `raw` 列留着原始 OneBot 事件，
    所以能从里面捞回来。
    """
    event = row.get("raw") or {}
    if not isinstance(event, Mapping):
        event = {}
    return {
        "message_id": str(row.get("message_id") or ""),
        "group_id": str(row.get("group_id") or ""),
        "group_name": row.get("group_name"),
        "sender_id": str(row.get("sender_id") or ""),
        "sender_name": row.get("sender_name"),
        "ts": int(row.get("ts") or now_ms()),
        "content": row.get("content") or "",
        "text": row.get("content") or "",
        "attachments": list(row.get("attachments") or []),
        "at_all": bool(event.get("at_all")),
        "mentions": list(event.get("mentions") or []),
        "reply_to": event.get("reply_to"),
        "raw": dict(event) if isinstance(event, dict) else {},
    }


def attachments_from_row(row: Mapping[str, Any]) -> list[dict]:
    """从 `raw` 列里的原始 OneBot 事件重建附件（只用于"附件还没上传就崩了"）。

    恢复时 attachments 列大概率是空的（写前日志那一刻还没下载），
    但原始事件里有图片 URL —— 不重建的话这些图就永远丢了。
    """
    event = row.get("raw") or {}
    if not isinstance(event, Mapping):
        return []
    try:
        parsed = parse_message(event.get("message") or event.get("raw_message") or [])
    except Exception:
        return []
    return attachments_payload(parsed.attachments)


# ---------------------------------------------------------------------------
# 附件：下载 → 上传 → 回填
# ---------------------------------------------------------------------------


def _get_media_client() -> httpx.AsyncClient:
    """媒体下载共用一个连接池。

    QQ 的图片 URL 是 HTTPS，每条消息都新建客户端会白白付 TLS 握手；
    而这个客户端**不碰后端**（后端走 BackendClient），所以生命周期分开是安全的。
    """
    global _media_client
    if _media_client is None or _media_client.is_closed:
        _media_client = httpx.AsyncClient(timeout=MEDIA_TIMEOUT, follow_redirects=True)
    return _media_client


async def close_media_client() -> None:
    global _media_client
    if _media_client is not None and not _media_client.is_closed:
        await _media_client.aclose()
    _media_client = None


@dataclass
class PreparedAttachments:
    """一次消息的附件处理结果。

    两件事必须一起算：图片既要**上传给后端存**（前端要能看），
    又要**喂给多模态模型**（DDL 可能写在图里）。分开处理会把同一张图下两遍。
    """

    payload: list[dict] = field(default_factory=list)
    image_data_urls: list[str] = field(default_factory=list)


def _degraded_attachment(att: Mapping[str, Any], source_url: str | None, note: str) -> dict:
    """上传/下载失败时的附件行：**只存 source_url**，绝不因为一张图丢掉整条通知。"""
    return {
        "id": None,
        "type": att.get("type"),
        "name": att.get("name"),
        "size": att.get("size"),
        # 上传成功时 url 是 `/api/attachments/xxx`（前端走 /api 代理），
        # 失败时退化成 QQ CDN 的原始地址 —— 前端照样能直接打开，信息不丢。
        "url": source_url,
        "source_url": source_url,
        "degraded": True,
        "note": note,
    }


async def _download(url: str, max_bytes: int) -> tuple[bytes | None, str | None, str | None]:
    """下载字节。返回 (内容, content_type, 失败原因)。**不抛异常。**"""
    client = _get_media_client()
    try:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                return None, None, f"HTTP {resp.status_code}"
            ctype = (resp.headers.get("content-type") or "").split(";")[0].strip() or None
            buf = bytearray()
            async for chunk in resp.aiter_bytes():
                buf.extend(chunk)
                if len(buf) > max_bytes:
                    # 边下边判：不然一个 500MB 的附件会先把内存吃光
                    return None, ctype, f"超过 MEDIA_MAX_BYTES={max_bytes}"
            return bytes(buf), ctype, None
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"


def _guess_filename(att: Mapping[str, Any], url: str | None) -> str:
    name = (att.get("name") or "").strip()
    if name:
        return name
    if url:
        tail = url.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
        if tail and "." in tail:
            return tail
    return "attachment"


async def prepare_attachments(
    raw: Mapping[str, Any],
    backend: BackendClient,
    settings: Settings,
) -> PreparedAttachments:
    """下载每条附件并上传给后端；同时（按需）为多模态模型准备 data URL。"""
    out = PreparedAttachments()
    want_images = bool(settings.vlm_enabled)
    for att in raw.get("attachments") or []:
        if not isinstance(att, Mapping):
            continue
        att_type = str(att.get("type") or "")
        source_url = att.get("url") or None
        name = _guess_filename(att, source_url)

        if att_type not in DOWNLOADABLE_TYPES or not source_url:
            out.payload.append(_degraded_attachment(att, source_url, "没有可下载的字节"))
            continue
        if not settings.media_download_enabled:
            out.payload.append(_degraded_attachment(att, source_url, "MEDIA_DOWNLOAD_ENABLED=false"))
            continue

        content, ctype, error = await _download(str(source_url), settings.media_max_bytes)
        if content is None:
            logger.warning("附件下载失败 %s: %s", name, error)
            out.payload.append(_degraded_attachment(att, source_url, f"下载失败：{error}"))
            continue

        mime = ctype or mimetypes.guess_type(name)[0] or "application/octet-stream"
        uploaded = await backend.upload_attachment(
            filename=name, content=content, content_type=mime, source_url=str(source_url)
        )
        if not uploaded:
            out.payload.append(_degraded_attachment(att, source_url, "上传失败"))
            continue

        out.payload.append(
            {
                "id": uploaded.get("id"),
                "type": att_type,
                "name": name,
                "size": uploaded.get("size") or len(content),
                "url": uploaded.get("url"),
                "source_url": str(source_url),
            }
        )

        if want_images and att_type == "image" and len(out.image_data_urls) < settings.vlm_max_images:
            b64 = base64.b64encode(content).decode("ascii")
            out.image_data_urls.append(f"data:{mime};base64,{b64}")

    if want_images and not out.image_data_urls and raw.get("attachments"):
        logger.debug("VLM 已启用，但这条消息没有可用的图片字节")
    return out


# ---------------------------------------------------------------------------
# 抽取
# ---------------------------------------------------------------------------


def _merge_rule_disagreement(
    llm_result: dict | None, rule_result: dict | None, model: str
) -> dict | None:
    """模型说"不是通知"，但规则认为有明确时间 → 保留条目并标冲突。

    方向是刻意的：宁可多推一条让人一键否决，也不能漏掉一条真通知。
    """
    if llm_result is not None:
        return llm_result
    if rule_result is None:
        return None

    merged = dict(rule_result)
    merged["conflict"] = True
    merged["due_confidence"] = min(float(merged.get("due_confidence") or 0), 0.5)
    merged["candidates"] = [
        {"model": "rule-engine", "due_at": merged.get("due_at"), "due_text": merged.get("due_text")},
        {"model": model, "due_at": None, "due_text": None, "note": "模型判定为非通知"},
    ]
    return merged


async def parse_content(
    raw: Mapping[str, Any],
    settings: Settings,
    images: Iterable[str] = (),
) -> tuple[dict | None, bool, int]:
    """按 EXTRACTOR 配置抽取。返回 (结果, 是否降级, 消耗 token)。

    **不抛异常**：LLM 那条路整体失败就降级到规则，并把这件事记下来。
    """
    content = str(raw.get("content") or raw.get("text") or "")
    ts = int(raw.get("ts") or now_ms())
    rule_result = rule_extract(content, ts, at_all=bool(raw.get("at_all")))

    if settings.extractor == "rule":
        # 核心链路在 EXTRACTOR=rule 时完全不发任何网络请求给模型厂商
        return rule_result, False, 0

    try:
        out = await extract_with_llm(dict(raw), list(images))
    except Exception as exc:
        logger.error(
            "LLM 抽取失败，降级为规则抽取 msg_id=%s: %s", raw.get("message_id"), exc
        )
        return rule_result, True, 0

    tokens = int(out.get("tokens") or 0)
    result = _merge_rule_disagreement(
        out.get("result"), rule_result, target_from_settings(settings, "primary").label
    )
    return result, False, tokens


def build_notification_payload(
    raw_id: str, raw: Mapping[str, Any], result: Mapping[str, Any]
) -> dict | None:
    """抽取结果 → `POST /api/notifications` 的请求体。证据为空则返回 None。

    **硬约束：evidence 必须非空。** 没有证据的条目一律不建 —— 后端也替
    bot 守着这条线（空 evidence 直接 400）。
    """
    evidence = str(result.get("evidence") or "").strip()
    if not evidence:
        return None
    return {
        "raw_message_id": raw_id,
        "group_id": str(raw.get("group_id") or ""),
        "group_name": raw.get("group_name"),
        "sender_id": str(raw.get("sender_id") or ""),
        "sender_name": raw.get("sender_name"),
        "source_ts": int(raw.get("ts") or now_ms()),
        "title": result.get("title"),
        "summary": result.get("summary"),
        "location": result.get("location"),
        "due_at": result.get("due_at"),
        "due_text": result.get("due_text"),
        "due_confidence": float(result.get("due_confidence") or 0.0),
        "evidence": evidence,
        "conflict": bool(result.get("conflict")),
        "candidates": list(result.get("candidates") or []),
        "extractor": result.get("extractor"),
        "model": result.get("model"),
        "prompt_ver": result.get("prompt_ver"),
    }


# ---------------------------------------------------------------------------
# 后端写入的小工具（都不让"顺手的一步"拖垮整条链路）
# ---------------------------------------------------------------------------


async def _patch_state(
    backend: BackendClient,
    raw_id: str,
    state: str,
    reason: str | None = None,
    attachments: list[dict] | None = None,
) -> None:
    payload: dict = {"state": state, "state_reason": reason}
    if attachments is not None:
        payload["attachments"] = attachments
    try:
        await backend.patch_message(raw_id, payload)
    except BackendError as exc:
        logger.warning("更新 raw 状态失败 raw=%s state=%s: %s", raw_id, state, exc)


async def _bump_stats(backend: BackendClient, fields: dict[str, int]) -> None:
    """统计写失败只记 warning：它不该让一条已经入库的通知变成"失败"。"""
    fields = {k: int(v) for k, v in fields.items() if v}
    if not fields:
        return
    try:
        await backend.add_stats(local_day(), fields)
    except BackendError as exc:
        logger.warning("写统计失败 fields=%s: %s", fields, exc)


def _outcome_stats(
    *, is_new: bool, outcome: str, degraded: bool, conflict: bool, tokens: int
) -> dict[str, int]:
    stats: dict[str, int] = {}
    if is_new:
        stats["ingested"] = 1
    if outcome == "extracted":
        stats["extracted"] = 1
    elif outcome == "unparsed":
        stats["unparsed"] = 1
    elif outcome == "degraded":
        stats["degraded"] = 1
        stats["unparsed"] = 1
    if conflict:
        stats["conflicts"] = 1
    if degraded and outcome == "extracted":
        # 降级但规则兜住了：条目建了，也要让人知道这次没有模型把关
        stats["degraded"] = 1
    if tokens:
        stats["llm_tokens"] = tokens
    return stats


# ---------------------------------------------------------------------------
# 缺口检测（消息间隔）
# ---------------------------------------------------------------------------


async def _maybe_gap_alert(
    backend: BackendClient,
    settings: Settings,
    raw: Mapping[str, Any],
    previous_last_msg_ts: Any,
) -> None:
    """两条消息间隔过久 → 记一条缺口告警。

    这是"断线期间的通知已经永久丢了"的唯一证据：NapCat 靠实时推送，
    重启后一切看起来都正常，只有时间戳能证明中间缺了一段。
    """
    try:
        previous = int(previous_last_msg_ts) if previous_last_msg_ts else None
    except (TypeError, ValueError):
        previous = None
    if not previous:
        return

    current = int(raw.get("ts") or 0)
    gap_ms = current - previous
    if gap_ms <= settings.gap_alert_hours * 3600 * 1000:
        return
    hours = round(gap_ms / 3600000, 1)
    try:
        await backend.create_gap_alert(
            group_id=str(raw.get("group_id") or ""),
            group_name=raw.get("group_name"),
            from_ts=previous,
            to_ts=current,
            reason=f"两条消息间隔 {hours} 小时，此期间的通知可能已永久丢失",
        )
    except BackendError as exc:
        logger.warning("写缺口告警失败：%s", exc)
        return
    logger.warning(
        "群 %s(%s) 两条消息间隔 %.1f 小时，已生成缺口告警",
        raw.get("group_name"),
        raw.get("group_id"),
        hours,
    )


# ---------------------------------------------------------------------------
# 写前日志
# ---------------------------------------------------------------------------


@dataclass
class WriteAhead:
    """写前日志的结果。outcome != "ok" 时调用方直接结束。"""

    outcome: str
    raw_id: str | None = None
    is_new: bool = True
    doc: dict = field(default_factory=dict)


async def write_ahead(
    msg: Any,
    backend: BackendClient,
    settings: Settings,
    *,
    retry: bool = False,
) -> WriteAhead:
    """【第一件落库的事】把原文写进后端，保住唯一不可再生的资产。

    这一步之前只做**纯本地的群白名单判断**（不失败、不耗时、不产生丢失窗口），
    除此之外不做任何网络动作 —— 附件下载和抽取都在它之后。
    """
    doc = _canonical(msg)

    if not settings.in_group_whitelist(doc.get("group_id")):
        # DEBUG 级别：这类消息量大且重复，INFO 会把日志刷爆
        log_message(message_meta(doc), "group_filtered", 原因="群不在白名单")
        return WriteAhead(outcome="group_filtered", doc=doc)

    # 写前日志：attachments 先留空，稍后 PATCH 回填（契约第 1 节）
    payload = build_message_payload(doc, [])
    try:
        body = await backend.create_message(payload)
    except BackendError as exc:
        backend.mark_for_retry(message=doc, note=f"写前日志失败：{exc}")
        log_message(doc, "error", 原因=f"写前日志失败：{exc}")
        return WriteAhead(outcome="error", doc=doc)

    raw_id = str(body.get("id") or "")
    if not raw_id:
        backend.mark_for_retry(message=doc, note="后端没有返回 id")
        log_message(doc, "error", 原因="后端没有返回 id")
        return WriteAhead(outcome="error", doc=doc)

    is_new = bool(body.get("is_new", True))
    if not is_new and not retry:
        # 幂等命中：同一条消息被推了两次（NapCat 重连时很常见）
        log_message(message_meta(doc), "duplicate", raw_id=raw_id)
        return WriteAhead(outcome="duplicate", raw_id=raw_id, is_new=False, doc=doc)

    return WriteAhead(outcome="ok", raw_id=raw_id, is_new=is_new, doc=doc)


# ---------------------------------------------------------------------------
# 后半段：附件 → 群状态 → 白名单 → 抽取 → 建条 → 统计 → 日志
# ---------------------------------------------------------------------------


async def finish_message(
    raw_id: str,
    doc: Mapping[str, Any],
    backend: BackendClient,
    *,
    settings: Settings | None = None,
    is_new: bool = True,
    skipped_attachments: bool = False,
    touch_group: bool = True,
) -> str:
    """原文已经在后端之后的所有步骤。

    `skipped_attachments`：崩溃恢复时，如果这条消息的附件已经回填过了就别再下一遍。
    `touch_group`：崩溃恢复时**不动**群状态 —— 恢复的是旧消息，把 last_msg_ts
    改回去会让缺口检测看到一条本不存在的"时间倒流"。
    """
    settings = settings or get_settings()
    doc = dict(doc)

    # ---- 附件：下载 → 上传 → PATCH 回填 ----
    if skipped_attachments:
        prep = PreparedAttachments(payload=list(doc.get("attachments") or []))
    else:
        prep = await prepare_attachments(doc, backend, settings)
        if prep.payload:
            # 只在真有附件时才多打一次 PATCH：写前日志已经存了 `attachments: []`，
            # 没有附件就没有要补的东西
            try:
                await backend.patch_message(raw_id, {"attachments": prep.payload})
            except BackendError as exc:
                # 回填失败不影响抽取：原文已经在了，附件只是"事后补齐"
                logger.warning("回填附件失败 raw=%s: %s", raw_id, exc)

    doc["attachments"] = prep.payload

    # ---- 群状态：缺口检测需要"上一条消息的时间" ----
    if touch_group:
        try:
            group_body = await backend.upsert_group(
                doc.get("group_id"), doc.get("group_name"), int(doc.get("ts") or now_ms())
            )
        except BackendError as exc:
            # 群状态只是辅助信息，挂了不该让这条通知丢掉
            logger.warning("upsert 群状态失败 group=%s: %s", doc.get("group_id"), exc)
        else:
            await _maybe_gap_alert(backend, settings, doc, group_body.get("previous_last_msg_ts"))

    # ---- 白名单 → 抽取 → 建条 → 统计 → 日志 ----
    return await process_raw(
        raw_id, doc, backend, settings=settings, images=prep.image_data_urls, is_new=is_new
    )


async def process_raw(
    raw_id: str,
    raw: Mapping[str, Any],
    backend: BackendClient,
    *,
    settings: Settings | None = None,
    images: Iterable[str] = (),
    is_new: bool = True,
) -> str:
    """发送者白名单 → 抽取 → 建条/标状态 → 统计 → 一行日志。

    抽出来单独一个函数，是因为这段**不依赖 OneBot**：只要有一行已入库的 raw
    就能跑（手动 /add 走的也是这条路）。返回结果字符串即日志里的 `结果=`。
    """
    settings = settings or get_settings()
    doc = _canonical(raw)

    try:
        # ---- 发送者白名单 ----
        if not settings.in_sender_whitelist(doc.get("sender_id")):
            await _patch_state(backend, raw_id, "skipped_whitelist", "发送者不在白名单")
            await _bump_stats(backend, _outcome_stats(
                is_new=is_new, outcome="skipped_whitelist",
                degraded=False, conflict=False, tokens=0,
            ))
            log_message(
                doc,
                "skipped_whitelist",
                raw_id=raw_id,
                发送者=f"{doc.get('sender_name')}({doc.get('sender_id')})",
            )
            return "skipped_whitelist"

        # ---- 抽取 ----
        result, degraded, tokens = await parse_content(doc, settings, images)

        if result is None:
            if degraded:
                # LLM 失败、规则也没兜住 —— 这是真的盲区
                reason = "LLM 失败且规则也无法解析"
                await _patch_state(backend, raw_id, "degraded", reason)
                await _bump_stats(backend, _outcome_stats(
                    is_new=is_new, outcome="degraded",
                    degraded=True, conflict=False, tokens=tokens,
                ))
                log_message(doc, "degraded", raw_id=raw_id, 原因=reason, 抽取器=settings.extractor)
                return "degraded"
            # 判定为闲聊/回执，属于正常结果，不该计入"未能解析"
            await _patch_state(backend, raw_id, "noise", "判定为非通知")
            await _bump_stats(backend, _outcome_stats(
                is_new=is_new, outcome="noise",
                degraded=False, conflict=False, tokens=tokens,
            ))
            log_message(doc, "noise", raw_id=raw_id, 抽取器=settings.extractor)
            return "noise"

        payload = build_notification_payload(raw_id, doc, result)
        if payload is None:
            # 硬约束：没有证据的条目宁可不要
            reason = "抽取结果缺少 evidence，已拒绝建条"
            await _patch_state(backend, raw_id, "unparsed", reason)
            await _bump_stats(backend, _outcome_stats(
                is_new=is_new, outcome="unparsed",
                degraded=degraded, conflict=False, tokens=tokens,
            ))
            log_message(doc, "unparsed", raw_id=raw_id, 原因=reason, 抽取器=settings.extractor)
            return "unparsed"

        # ---- 建条 ----
        try:
            await backend.create_notification(payload)
        except BackendRejected as exc:
            # 4xx：重试多少次结果都一样（比如 evidence 被后端拒了）。
            # 标成终态 `error`，否则恢复循环会每分钟重试一个永远失败的请求。
            reason = f"建通知被后端拒绝：{exc}"
            await _patch_state(backend, raw_id, "error", reason)
            log_message(doc, "error", raw_id=raw_id, 原因=reason)
            return "error"
        except BackendError as exc:
            # 暂时性失败：raw 停在 pending，恢复循环（启动 / 重连 / 每分钟）会把它
            # 捡回来重新走一遍。原文没丢，这里如实记一行 ERROR。
            logger.error("建通知失败，raw=%s 保持 pending 等待恢复：%s", raw_id, exc)
            log_message(doc, "error", raw_id=raw_id, 原因=f"建通知失败，等待恢复：{exc}")
            return "error"

        await _patch_state(backend, raw_id, "extracted")
        await _bump_stats(backend, _outcome_stats(
            is_new=is_new, outcome="extracted",
            degraded=degraded, conflict=bool(payload.get("conflict")), tokens=tokens,
        ))

        log_message(
            doc,
            "extracted",
            raw_id=raw_id,
            标题=payload.get("title"),
            截止=fmt_due(payload.get("due_at"), payload.get("due_text")),
            地点=payload.get("location"),
            置信度=payload.get("due_confidence"),
            冲突="是" if payload.get("conflict") else None,
            抽取器=payload.get("extractor"),
            模型=payload.get("model"),
            tokens=tokens or None,
            附件=len(doc.get("attachments") or []) or None,
        )
        return "extracted"

    except BackendError as exc:
        # 兜底：白名单 PATCH / 统计这些"顺手的一步"挂了，也不该让消息静默消失
        logger.error("处理消息时后端报错 raw=%s: %s", raw_id, exc)
        log_message(doc, "error", raw_id=raw_id, 原因=f"后端错误：{exc}")
        return "error"
    except Exception as exc:
        logger.exception("处理消息异常 raw=%s: %s", raw_id, exc)
        log_message(doc, "error", raw_id=raw_id, 原因=f"{type(exc).__name__}: {exc}")
        return "error"


async def ingest_message(
    msg: Any,
    backend: BackendClient,
    *,
    settings: Settings | None = None,
    retry: bool = False,
) -> str:
    """完整数据流（写前日志 + 后半段）。返回结果字符串，也是一行日志里的 `结果=`。"""
    settings = settings or get_settings()
    wa = await write_ahead(msg, backend, settings, retry=retry)
    if wa.outcome != "ok":
        return wa.outcome
    assert wa.raw_id
    return await finish_message(
        wa.raw_id, wa.doc, backend, settings=settings, is_new=wa.is_new
    )


async def resume_pending(
    backend: BackendClient,
    settings: Settings | None = None,
    *,
    limit: int = RECOVERY_LIMIT,
) -> int:
    """崩溃恢复：把后端里还停在 `pending` 的消息捡回来继续走。

    这是"绝不静默丢弃"的最后一道保险 —— 写前日志保证了原文不会丢，
    这个函数保证原文**不会永远停在那里没人管**。

    调用时机：bot 启动时、OneBot 重连成功后（见 main.BotRuntime）。
    后端不可达时返回 0 并记 warning，不抛异常。
    """
    settings = settings or get_settings()
    try:
        rows = await backend.list_messages(state="pending", limit=limit)
    except BackendError as exc:
        logger.warning("崩溃恢复：读取 pending 消息失败，跳过本轮：%s", exc)
        return 0

    if not rows:
        return 0

    # 按 ts 正序处理：群状态 / 缺口检测都依赖时间顺序
    rows.sort(key=lambda r: int(r.get("ts") or 0))
    logger.info("崩溃恢复：发现 %d 条 pending 消息，开始补处理", len(rows))

    done = 0
    for row in rows:
        raw_id = str(row.get("id") or "")
        if not raw_id:
            continue
        doc = doc_from_row(row)
        stored = list(row.get("attachments") or [])
        if not stored:
            # 写前日志那一刻还没下载附件，原始事件里的 URL 是唯一的线索
            doc["attachments"] = attachments_from_row(row)
        try:
            outcome = await finish_message(
                raw_id,
                doc,
                backend,
                settings=settings,
                is_new=False,          # 原文早就在库里了，不能再记一次 ingested
                skipped_attachments=bool(stored),
                touch_group=False,     # 恢复的是旧消息，不能把 last_msg_ts 拨回去
            )
        except Exception as exc:  # 单条失败不能拖垮整轮恢复
            logger.exception("崩溃恢复失败 raw=%s: %s", raw_id, exc)
            continue
        logger.info("崩溃恢复：raw=%s → %s", raw_id, outcome)
        done += 1

    return done


async def create_manual_notification(
    *,
    text: str,
    sender_id: str,
    sender_name: str,
    result: Mapping[str, Any] | None,
    backend: BackendClient,
    confirmed: bool = False,
    ts: int | None = None,
) -> dict:
    """手动 /add 建条：POST /api/messages + POST /api/notifications。

    手动任务没有"群消息"这个客观事实，所以不走进群白名单、也不做缺口检测；
    但**入库的形状和群消息完全一样**，这样前端只需要认识一种数据。
    """
    stamp = int(ts or now_ms())
    raw = {
        "message_id": f"manual-{stamp}",
        "group_id": f"manual:{sender_id or MANUAL_SENDER_FALLBACK}",
        "group_name": MANUAL_GROUP_NAME,
        "sender_id": sender_id or MANUAL_SENDER_FALLBACK,
        "sender_name": sender_name or sender_id or MANUAL_SENDER_FALLBACK,
        "ts": stamp,
        "content": text,
        "text": text,
        "attachments": [],
        "at_all": False,
        "raw": {"manual": True, "confirmed": bool(confirmed)},
    }
    doc = _canonical(raw)
    payload = build_message_payload(doc, [])
    body = await backend.create_message(payload)
    raw_id = str(body.get("id") or "")
    if not raw_id:
        raise BackendError("后端没有返回原消息 id")

    safe_result = dict(result or {})
    # evidence 硬约束：手动输入本身就是证据（用户亲手打的字）
    if not str(safe_result.get("evidence") or "").strip():
        safe_result["evidence"] = text[:200]
    if not str(safe_result.get("title") or "").strip():
        safe_result["title"] = text[:60]
    if not safe_result.get("extractor"):
        safe_result["extractor"] = "manual"

    notif_payload = build_notification_payload(raw_id, doc, safe_result)
    assert notif_payload is not None  # evidence 上面已兜住
    created = await backend.create_notification(notif_payload)

    await _patch_state(backend, raw_id, "extracted")
    await _bump_stats(
        backend,
        _outcome_stats(
            is_new=True,
            outcome="extracted",
            degraded=False,
            conflict=bool(safe_result.get("conflict")),
            tokens=int(safe_result.get("tokens") or 0),
        ),
    )
    log_message(
        doc,
        "created",
        raw_id=raw_id,
        标题=notif_payload.get("title"),
        截止=fmt_due(notif_payload.get("due_at"), notif_payload.get("due_text")),
        地点=notif_payload.get("location"),
        确认="是" if confirmed else None,
    )
    out = dict(created or {})
    out.setdefault("id", raw_id)
    out["raw_message_id"] = raw_id
    for key in ("title", "due_at", "due_text", "due_confidence", "location", "evidence"):
        out[key] = notif_payload.get(key)
    return out
