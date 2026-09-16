"""本地假后端：按契约 docs/api.md 实现的**内存版后端**，用来在没有真后端时验证 bot。

    python -m app.tools.fake_backend --port 9000
    python -m app.tools.fake_backend --port 9000 --fail-first 2   # 前 2 次 POST /api/messages 返回 500
    python -m app.tools.fake_backend --port 9000 --api-token secret

它实现了契约里除"前端专用"之外的全部接口：

  raw_message : POST/GET/PATCH /api/messages[/{id}]
  notification: POST/GET/PATCH/DELETE /api/notifications[/{id}]、corrections、read
  attachment  : POST /api/attachments（multipart）、GET /api/attachments/{id}
  group_state : POST/GET /api/groups
  gap_alert   : POST/GET /api/gap-alerts、POST /api/gap-alerts/{id}/ack
  pipeline_stat: POST/GET /api/stats
  digest_log  : POST/GET /api/digest-log
  bot_state   : PUT/GET/DELETE /api/state/{ns}/{key}、GET /api/state/{ns}
  health      : GET /api/health

外加两个**只有假后端才有**的自检接口：
  GET  /api/_fake/state  —— 收到了什么、建了什么、调用序列
  POST /api/_fake/reset  —— 清空，便于一个进程里跑多组断言

`--fail-first N` 是专门为重试逻辑准备的：配上它跑
`python -m tests.check_pipeline_e2e`，就能在日志里看到 1s/2s/4s 的退避重试。

注意它**没有**依赖 python-multipart（这台机器上装不上）：multipart 由
`_parse_multipart()` 手工解析，够读一个文件字段 + 几个普通字段。
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Windows 控制台默认 GBK，日志里的中文会 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s fake-backend | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fake-backend")

TZ = timezone(timedelta(hours=8))

# 消息状态里，哪些算"已处理完"
DEFAULT_MEDIA_MAX_BYTES = 5 * 1024 * 1024


def _now_ms() -> int:
    return int(time.time() * 1000)


def _new_id() -> str:
    return f"{_now_ms():013d}{random.randint(0, 0xFFFFFF):06x}"


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------


class MessageBody(BaseModel):
    # 除这几个之外的字段一律忽略（契约：未知字段忽略，不报错）
    message_id: str = ""
    group_id: str = ""
    group_name: str | None = None
    sender_id: str = ""
    sender_name: str | None = None
    ts: int = 0
    content: str = ""
    attachments: list[dict] = []
    raw: dict = {}


class MessagePatchBody(BaseModel):
    state: str | None = None
    state_reason: str | None = None
    attachments: list[dict] | None = None


class NotificationBody(BaseModel):
    raw_message_id: str = ""
    group_id: str = ""
    group_name: str | None = None
    sender_id: str = ""
    sender_name: str | None = None
    source_ts: int = 0
    title: str | None = None
    summary: str | None = None
    location: str | None = None
    due_at: int | None = None
    due_text: str | None = None
    due_confidence: float = 0.0
    evidence: str = ""
    conflict: bool = False
    candidates: list[dict] = []
    extractor: str | None = None
    model: str | None = None
    prompt_ver: str | None = None


class NotificationPatchBody(BaseModel):
    title: str | None = None
    summary: str | None = None
    location: str | None = None
    due_at: int | None = None
    due_text: str | None = None
    due_confidence: float | None = None
    evidence: str | None = None
    conflict: bool | None = None
    candidates: list[dict] | None = None
    model: str | None = None
    prompt_ver: str | None = None


class CorrectionBody(BaseModel):
    field: str
    value: Any = None
    user_id: str = "web"


class ReadBody(BaseModel):
    read: bool = True


class GroupBody(BaseModel):
    group_id: str = ""
    group_name: str | None = None
    last_msg_ts: int = 0


class GapAlertBody(BaseModel):
    group_id: str = ""
    group_name: str | None = None
    from_ts: int = 0
    to_ts: int = 0
    reason: str = ""


class StatsBody(BaseModel):
    day: str | None = None
    fields: dict[str, Any] = {}


class DigestLogBody(BaseModel):
    day: str | None = None
    kind: str = "auto"
    text: str = ""
    sent: bool = False
    error: str | None = None


class StateBody(BaseModel):
    value: Any = None
    ttl_seconds: int | None = None


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------


class State:
    def __init__(self) -> None:
        self.token: str = ""
        self.fail_first: int = 0
        self.fail_attachments: bool = False
        self.media_max_bytes: int = DEFAULT_MEDIA_MAX_BYTES
        self.reset()

    def reset(self) -> None:
        self.post_message_calls = 0
        self.calls: list[dict] = []
        self.messages: dict[str, dict] = {}
        self.message_index: dict[tuple[str, str], str] = {}
        self.patched_messages: list[dict] = []
        self.notifications: dict[str, dict] = {}
        self.notif_by_raw: dict[str, str] = {}
        self.corrections: dict[str, dict] = {}
        self.reads: set[str] = set()
        self.attachment_blobs: dict[str, dict] = {}
        self.groups: dict[str, dict] = {}
        self.gap_alerts: dict[str, dict] = {}
        self.stats: dict[str, dict] = {}
        self.digest_logs: list[dict] = []
        self.kv: dict[tuple[str, str], dict] = {}
        self.blob_hits: list[str] = []


STATE = State()
app = FastAPI(title="Xcollector fake backend")


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _day_of(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=TZ).strftime("%Y-%m-%d")


def _note(request: Request, body: Any) -> None:
    """把请求体挂到这次调用的记录上，供 /api/_fake/state 断言。"""
    for entry in reversed(STATE.calls):
        if (
            entry.get("path") == request.url.path
            and entry.get("method") == request.method
            and entry.get("request_body") is None
        ):
            entry["request_body"] = body
            return


@app.middleware("http")
async def record_and_auth(request: Request, call_next):
    entry = {
        "method": request.method,
        "path": request.url.path,
        "query": str(request.url.query),
        "ts": _now_ms(),
        "status": None,
        "authorization": request.headers.get("authorization", ""),
        "request_body": None,
    }
    STATE.calls.append(entry)

    if STATE.token:
        if request.headers.get("authorization", "") != f"Bearer {STATE.token}":
            entry["status"] = 401
            return JSONResponse(status_code=401, content={"detail": "无效的 API_TOKEN"})

    response = await call_next(request)
    entry["status"] = response.status_code
    return response


def _parse_multipart(body: bytes, content_type: str) -> dict[str, Any]:
    """够用的 multipart/form-data 解析器（只处理单层、无嵌套）。"""
    match = re.search(r'boundary="?([^";]+)"?', content_type or "")
    if not match:
        return {}
    boundary = match.group(1).strip().encode()
    out: dict[str, Any] = {}
    for part in body.split(b"--" + boundary):
        if not part.strip() or part.strip() == b"--":
            continue
        head, _, data = part.partition(b"\r\n\r\n")
        if not _:
            continue
        if data.endswith(b"\r\n"):
            data = data[:-2]
        name_m = re.search(rb'name="([^"]*)"', head)
        if not name_m:
            continue
        name = name_m.group(1).decode("utf-8", "replace")
        file_m = re.search(rb'filename="([^"]*)"', head)
        type_m = re.search(rb"Content-Type:\s*([^\r\n]+)", head)
        if file_m:
            out[name] = {
                "filename": file_m.group(1).decode("utf-8", "replace"),
                "content_type": (type_m.group(1).decode() if type_m else "application/octet-stream"),
                "content": data,
            }
        else:
            out[name] = data.decode("utf-8", "replace")
    return out


# ---------------------------------------------------------------------------
# 读投影
# ---------------------------------------------------------------------------


def _effective(notif: dict) -> dict:
    corr = STATE.corrections.get(notif["id"], {})
    due_at = notif.get("due_at")
    due_text = notif.get("due_text")
    title = notif.get("title")
    summary = notif.get("summary")
    location = notif.get("location")

    if "due_at" in corr:
        try:
            due_at = int(float(corr["due_at"])) if corr["due_at"] is not None else None
        except (TypeError, ValueError):
            due_at = None
    if "due_text" in corr:
        due_text = corr["due_text"]
    if "title" in corr and corr["title"]:
        title = corr["title"]
    if "summary" in corr:
        summary = corr["summary"]
    if "location" in corr:
        location = corr["location"] or None

    if corr.get("status"):
        status = corr["status"]
    elif due_at is not None and due_at < _now_ms():
        status = "expired"
    else:
        status = "active"

    raw = STATE.messages.get(notif.get("raw_message_id") or "", {})
    return {
        "id": notif["id"],
        "group_id": notif.get("group_id"),
        "group_name": notif.get("group_name"),
        "sender_id": notif.get("sender_id"),
        "sender_name": notif.get("sender_name"),
        "title": title,
        "summary": summary,
        "location": location,
        "due_at": due_at,
        "due_text": due_text,
        "due_confidence": notif.get("due_confidence") or 0.0,
        "conflict": bool(notif.get("conflict")),
        "candidates": notif.get("candidates") or [],
        "evidence": notif.get("evidence") or "",
        "status": status,
        "manually_edited": bool(corr),
        "read": notif["id"] in STATE.reads,
        "attachments": raw.get("attachments") or [],
        "extractor": notif.get("extractor"),
        "model": notif.get("model"),
        "prompt_ver": notif.get("prompt_ver"),
        "source_ts": notif.get("source_ts"),
        "created_at": notif.get("created_at"),
        "updated_at": notif.get("updated_at"),
    }


def _state_live(key: tuple[str, str]) -> dict | None:
    row = STATE.kv.get(key)
    if row is None:
        return None
    expires = row.get("expires_at")
    if expires is not None and int(expires) <= _now_ms():
        return None  # 读取时判过期是必须的，清理只是省空间
    return row


# ---------------------------------------------------------------------------
# 1. raw_message
# ---------------------------------------------------------------------------


@app.post("/api/messages")
async def create_message(body: MessageBody, request: Request):
    _note(request, body.model_dump())
    STATE.post_message_calls += 1
    if STATE.post_message_calls <= STATE.fail_first:
        logger.warning(
            "第 %d 次 POST /api/messages → 故意返回 500（--fail-first %d）",
            STATE.post_message_calls,
            STATE.fail_first,
        )
        return JSONResponse(status_code=500, content={"detail": "模拟后端故障"})

    key = (body.group_id, body.message_id)
    existing = STATE.message_index.get(key)
    if existing:
        logger.info("幂等命中 msg_id=%s → 已有 raw=%s", body.message_id, existing)
        return {"id": existing, "is_new": False}

    raw_id = _new_id()
    row = {
        "id": raw_id,
        "message_id": body.message_id,
        "group_id": body.group_id,
        "group_name": body.group_name,
        "sender_id": body.sender_id,
        "sender_name": body.sender_name,
        "ts": body.ts or _now_ms(),
        "content": body.content,
        "attachments": list(body.attachments or []),
        "raw": body.raw or {},
        "state": "pending",
        "state_reason": None,
        "created_at": _now_ms(),
        "updated_at": _now_ms(),
    }
    STATE.messages[raw_id] = row
    STATE.message_index[key] = raw_id
    logger.info(
        "已存原文 raw=%s 群=%s msg_id=%s 正文=%s",
        raw_id,
        body.group_id,
        body.message_id,
        (body.content or "").replace("\n", " ")[:60],
    )
    return {"id": raw_id, "is_new": True}


@app.get("/api/messages")
async def list_messages(
    request: Request,
    state: list[str] | None = Query(default=None),
    group_id: str | None = Query(default=None),
    since: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    count_only: int = Query(default=0),
):
    wanted: set[str] = set()
    for chunk in state or []:
        wanted.update(s.strip() for s in str(chunk).split(",") if s.strip())

    rows = []
    for row in STATE.messages.values():
        if wanted and row["state"] not in wanted:
            continue
        if group_id and str(row["group_id"]) != str(group_id):
            continue
        if since is not None and int(row["ts"]) < int(since):
            continue
        rows.append(row)
    rows.sort(key=lambda r: (r["ts"], r["id"]))
    if count_only:
        return {"count": len(rows)}
    return {"messages": rows[:limit]}


@app.get("/api/messages/{raw_id}")
async def get_message(raw_id: str):
    row = STATE.messages.get(raw_id)
    if row is None:
        raise HTTPException(status_code=404, detail="消息不存在")
    return row


@app.patch("/api/messages/{raw_id}")
async def patch_message(raw_id: str, body: MessagePatchBody, request: Request):
    _note(request, body.model_dump(exclude_none=True))
    row = STATE.messages.get(raw_id)
    if row is None:
        raise HTTPException(status_code=404, detail="消息不存在")
    # 只有 state / state_reason / attachments 可改，其余一律忽略（契约第 1 节）
    if body.state is not None:
        row["state"] = body.state
    if body.state_reason is not None:
        row["state_reason"] = body.state_reason
    if body.attachments is not None:
        row["attachments"] = list(body.attachments)
    row["updated_at"] = _now_ms()
    logger.info("PATCH raw=%s state=%s 附件=%d", raw_id, row["state"], len(row["attachments"]))
    STATE.patched_messages.append({"id": raw_id, "state": row["state"], "attachments": row["attachments"]})
    return row


# ---------------------------------------------------------------------------
# 2. notification
# ---------------------------------------------------------------------------


@app.post("/api/notifications")
async def create_notification(body: NotificationBody, request: Request):
    _note(request, body.model_dump())
    if not (body.evidence or "").strip():
        # 契约：由后端替 bot 守住"没有证据的条目不许入库"
        raise HTTPException(status_code=400, detail="evidence 不能为空")

    existing = STATE.notif_by_raw.get(body.raw_message_id)
    if existing:
        row = STATE.notifications[existing]
        for field, value in body.model_dump().items():
            if field == "raw_message_id":
                continue
            if value is not None:
                row[field] = value
        row["updated_at"] = _now_ms()
        logger.info("通知已存在（幂等更新）notif=%s raw=%s", existing, body.raw_message_id)
        return {"id": existing, "created": False}

    notif_id = _new_id()
    row = body.model_dump()
    row["id"] = notif_id
    row["created_at"] = _now_ms()
    row["updated_at"] = _now_ms()
    STATE.notifications[notif_id] = row
    STATE.notif_by_raw[body.raw_message_id] = notif_id
    logger.info(
        "已建通知 notif=%s 标题=%s 截止=%s 来源群=%s",
        notif_id,
        body.title,
        body.due_text or body.due_at,
        body.group_id,
    )
    return {"id": notif_id, "created": True}


@app.get("/api/notifications")
async def list_notifications(
    since: int | None = Query(default=None),
    status: str = Query(default="all"),
    q: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    count_only: int = Query(default=0),
):
    views = [_effective(n) for n in STATE.notifications.values()]
    if since is not None:
        views = [v for v in views if int(v.get("updated_at") or 0) > int(since)]
    if status and status != "all":
        views = [v for v in views if v["status"] == status]
    if q:
        needle = q.strip().lower()
        views = [
            v
            for v in views
            if needle in (v.get("title") or "").lower()
            or needle in (v.get("summary") or "").lower()
            or needle in (v.get("evidence") or "").lower()
        ]
    views.sort(key=lambda v: (v.get("due_at") is None, v.get("due_at") or 0, -(v.get("source_ts") or 0)))
    if count_only:
        return {"count": len(views)}
    return {"notifications": views[:limit], "server_time": _now_ms()}


@app.get("/api/notifications/{notif_id}")
async def get_notification(notif_id: str):
    row = STATE.notifications.get(notif_id)
    if row is None:
        raise HTTPException(status_code=404, detail="通知不存在")
    return {
        "notification": _effective(row),
        "raw": STATE.messages.get(row.get("raw_message_id") or "", {}),
    }


@app.patch("/api/notifications/{notif_id}")
async def patch_notification(notif_id: str, body: NotificationPatchBody, request: Request):
    _note(request, body.model_dump(exclude_none=True))
    row = STATE.notifications.get(notif_id)
    if row is None:
        raise HTTPException(status_code=404, detail="通知不存在")
    # status / read 不允许在这里改（只能走 corrections / read），传了忽略
    for field, value in body.model_dump(exclude_none=True).items():
        row[field] = value
    row["updated_at"] = _now_ms()
    return _effective(row)


@app.delete("/api/notifications/{notif_id}")
async def delete_notification(notif_id: str):
    row = STATE.notifications.pop(notif_id, None)
    if row is None:
        raise HTTPException(status_code=404, detail="通知不存在")
    STATE.notif_by_raw.pop(row.get("raw_message_id") or "", None)
    STATE.corrections.pop(notif_id, None)
    STATE.reads.discard(notif_id)
    return {"deleted": True}


@app.post("/api/notifications/{notif_id}/corrections")
async def corrections(notif_id: str, body: CorrectionBody, request: Request):
    _note(request, body.model_dump())
    if notif_id not in STATE.notifications:
        raise HTTPException(status_code=404, detail="通知不存在")
    if body.field not in {"title", "summary", "location", "due_at", "due_text", "status"}:
        raise HTTPException(status_code=400, detail=f"field 非法：{body.field}")
    if body.field == "status" and body.value not in ("active", "archived", "done"):
        raise HTTPException(status_code=400, detail=f"status 非法：{body.value}")
    STATE.corrections.setdefault(notif_id, {})[body.field] = body.value
    logger.info(
        "修正 notif=%s field=%s value=%s by=%s",
        notif_id,
        body.field,
        body.value,
        body.user_id,
    )
    return {"ok": True, "notification": _effective(STATE.notifications[notif_id])}


@app.get("/api/notifications/{notif_id}/corrections")
async def list_corrections(notif_id: str):
    if notif_id not in STATE.notifications:
        raise HTTPException(status_code=404, detail="通知不存在")
    corr = STATE.corrections.get(notif_id, {})
    return {
        "corrections": [
            {"field": f, "value": v, "user_id": "unknown"} for f, v in corr.items()
        ]
    }


@app.post("/api/notifications/{notif_id}/read")
async def mark_read(notif_id: str, body: ReadBody | None = None):
    if notif_id not in STATE.notifications:
        raise HTTPException(status_code=404, detail="通知不存在")
    want = True if body is None else bool(body.read)
    if want:
        STATE.reads.add(notif_id)
    else:
        STATE.reads.discard(notif_id)
    return {"read": want}


# ---------------------------------------------------------------------------
# 3. attachment
# ---------------------------------------------------------------------------


@app.post("/api/attachments")
async def upload_attachment(request: Request):
    content_type = request.headers.get("content-type", "")
    raw_body = await request.body()
    form = _parse_multipart(raw_body, content_type)
    file_part = form.get("file")
    if not isinstance(file_part, dict):
        raise HTTPException(status_code=400, detail="缺少 file 字段")
    content = file_part["content"]
    _note(
        request,
        {
            "filename": form.get("filename"),
            "source_url": form.get("source_url"),
            "size": len(content),
            "content_type": file_part.get("content_type"),
        },
    )
    if STATE.fail_attachments:
        return JSONResponse(status_code=500, content={"detail": "模拟附件服务故障"})
    if len(content) > STATE.media_max_bytes:
        raise HTTPException(status_code=413, detail="附件超过 MEDIA_MAX_BYTES")

    att_id = "att_" + _new_id()[13:]
    STATE.attachment_blobs[att_id] = {
        "content": content,
        "content_type": file_part.get("content_type") or "application/octet-stream",
        "filename": form.get("filename") or file_part.get("filename") or att_id,
        "source_url": form.get("source_url"),
    }
    logger.info(
        "附件已上传 id=%s 文件名=%s 大小=%d source_url=%s",
        att_id,
        form.get("filename"),
        len(content),
        form.get("source_url"),
    )
    return {
        "id": att_id,
        "url": f"/api/attachments/{att_id}",
        "size": len(content),
        "content_type": STATE.attachment_blobs[att_id]["content_type"],
    }


@app.get("/api/attachments/{att_id}")
async def download_attachment(att_id: str):
    blob = STATE.attachment_blobs.get(att_id)
    if blob is None:
        raise HTTPException(status_code=404, detail="附件不存在")
    return Response(
        content=blob["content"],
        media_type=blob["content_type"],
        headers={"Content-Disposition": f'inline; filename="{blob["filename"]}"'},
    )


# ---------------------------------------------------------------------------
# 4. group_state
# ---------------------------------------------------------------------------


@app.post("/api/groups")
async def upsert_group(body: GroupBody, request: Request):
    _note(request, body.model_dump())
    gid = str(body.group_id)
    today = _day_of(_now_ms())
    existing = STATE.groups.get(gid)
    previous = existing.get("last_msg_ts") if existing else None

    if existing is None:
        row = {
            "group_id": gid,
            "group_name": body.group_name,
            "last_msg_ts": body.last_msg_ts,
            "msg_count_today": 1,
            "count_date": today,
        }
    else:
        row = dict(existing)
        row["group_name"] = body.group_name or row.get("group_name")
        row["last_msg_ts"] = max(int(row.get("last_msg_ts") or 0), int(body.last_msg_ts))
        if row.get("count_date") != today:
            row["count_date"] = today
            row["msg_count_today"] = 1
        else:
            row["msg_count_today"] = int(row.get("msg_count_today") or 0) + 1
    STATE.groups[gid] = row
    return {"group": row, "previous_last_msg_ts": previous}


@app.get("/api/groups")
async def list_groups():
    return {"groups": list(STATE.groups.values())}


# ---------------------------------------------------------------------------
# 5. gap_alert
# ---------------------------------------------------------------------------


@app.post("/api/gap-alerts")
async def create_gap_alert(body: GapAlertBody, request: Request):
    _note(request, body.model_dump())
    gap_id = "gap_" + _new_id()[13:]
    STATE.gap_alerts[gap_id] = {
        "id": gap_id,
        "group_id": body.group_id,
        "group_name": body.group_name,
        "from_ts": body.from_ts,
        "to_ts": body.to_ts,
        "reason": body.reason,
        "acknowledged": False,
        "created_at": _now_ms(),
    }
    logger.info("缺口告警 %s 群=%s %s", gap_id, body.group_id, body.reason)
    return {"id": gap_id}


@app.get("/api/gap-alerts")
async def list_gap_alerts(
    acknowledged: str | None = Query(default=None), limit: int = Query(default=20)
):
    rows = list(STATE.gap_alerts.values())
    if acknowledged is not None:
        want = str(acknowledged).lower() in ("1", "true", "yes")
        rows = [r for r in rows if bool(r["acknowledged"]) == want]
    rows.sort(key=lambda r: -(r.get("created_at") or 0))
    return {"alerts": rows[:limit]}


@app.post("/api/gap-alerts/{gap_id}/ack")
async def ack_gap_alert(gap_id: str):
    row = STATE.gap_alerts.get(gap_id)
    if row is None:
        raise HTTPException(status_code=404, detail="告警不存在")
    row["acknowledged"] = True
    return {"acknowledged": True}


# ---------------------------------------------------------------------------
# 6. pipeline_stat
# ---------------------------------------------------------------------------


@app.post("/api/stats")
async def add_stats(body: StatsBody, request: Request):
    _note(request, body.model_dump())
    day = body.day or _day_of(_now_ms())
    row = STATE.stats.setdefault(day, {"day": day})
    for field, value in (body.fields or {}).items():
        try:
            row[field] = int(row.get(field) or 0) + int(value)
        except (TypeError, ValueError):
            row[field] = value
    return row


@app.get("/api/stats")
async def get_stats(day: str | None = Query(default=None)):
    key = day or _day_of(_now_ms())
    return STATE.stats.get(key, {"day": key})


# ---------------------------------------------------------------------------
# 10. digest_log
# ---------------------------------------------------------------------------


@app.post("/api/digest-log")
async def add_digest_log(body: DigestLogBody, request: Request):
    _note(request, body.model_dump())
    log_id = _new_id()
    STATE.digest_logs.append(
        {
            "id": log_id,
            "day": body.day or _day_of(_now_ms()),
            "kind": body.kind,
            "text": body.text,
            "sent": bool(body.sent),
            "error": body.error,
            "ts": _now_ms(),
        }
    )
    return {"id": log_id}


@app.get("/api/digest-log")
async def list_digest_logs(
    day: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    sent: str | None = Query(default=None),
    limit: int = Query(default=50),
    count_only: int = Query(default=0),
):
    rows = list(STATE.digest_logs)
    if day:
        rows = [r for r in rows if r["day"] == day]
    if kind:
        rows = [r for r in rows if r["kind"] == kind]
    if sent is not None:
        want = str(sent).lower() in ("1", "true", "yes")
        rows = [r for r in rows if bool(r["sent"]) == want]
    rows.sort(key=lambda r: -(r.get("ts") or 0))
    if count_only:
        return {"count": len(rows)}
    return {"logs": rows[:limit]}


# ---------------------------------------------------------------------------
# 11. bot_state
# ---------------------------------------------------------------------------


@app.put("/api/state/{namespace}/{key}")
async def put_state(namespace: str, key: str, body: StateBody, request: Request):
    _note(request, {"value": body.value, "ttl_seconds": body.ttl_seconds})
    expires_at = None
    if body.ttl_seconds is not None:
        expires_at = _now_ms() + int(body.ttl_seconds) * 1000
    STATE.kv[(namespace, key)] = {
        "key": key,
        "value": body.value,
        "expires_at": expires_at,
        "updated_at": _now_ms(),
    }
    logger.info("bot_state 写入 %s/%s ttl=%s", namespace, key, body.ttl_seconds)
    return {"ok": True, "expires_at": expires_at}


@app.get("/api/state/{namespace}/{key}")
async def get_state(namespace: str, key: str):
    row = _state_live((namespace, key))
    if row is None:
        raise HTTPException(status_code=404, detail="不存在或已过期")
    return {"key": key, "value": row["value"], "expires_at": row.get("expires_at")}


@app.delete("/api/state/{namespace}/{key}")
async def delete_state(namespace: str, key: str):
    existed = STATE.kv.pop((namespace, key), None) is not None
    return {"deleted": existed}


@app.get("/api/state/{namespace}")
async def list_state(namespace: str):
    items = []
    for (ns, key), row in list(STATE.kv.items()):
        if ns != namespace:
            continue
        if _state_live((ns, key)) is None:
            continue
        items.append({"key": key, "value": row["value"], "expires_at": row.get("expires_at")})
    return {"items": items}


# ---------------------------------------------------------------------------
# 7. health
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "server_time": _now_ms(),
        "storage": {"driver": "memory", "path": ":fake:", "writable": True},
        "counts": {
            "messages": len(STATE.messages),
            "notifications": len(STATE.notifications),
            "attachments": len(STATE.attachment_blobs),
        },
        "version": "0.2.0-fake",
    }


# ---------------------------------------------------------------------------
# 自检接口（只有假后端有）
# ---------------------------------------------------------------------------

# 1x1 透明 PNG：给"附件下载 + 上传"那条路一个真实可下的字节源。
# 真实世界里这个 URL 是 QQ CDN，测试里指向假后端自己。
FAKE_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c63000100000500010d0a2db4000000"
    "0049454e44ae426082"
)


@app.get("/api/_fake/blob/{name}")
async def fake_blob(name: str):
    """假的"QQ CDN 图片地址"，供 bot 下载（不属于契约接口）。"""
    if not name.endswith(".png"):
        raise HTTPException(status_code=404, detail="只有 .png")
    STATE.blob_hits.append(name)
    return Response(
        content=FAKE_PNG,
        media_type="image/png",
        headers={"Content-Disposition": f'inline; filename="{name}"'},
    )


@app.get("/api/_fake/state")
async def fake_state():
    """自检用：看假后端到底收到了什么、按什么顺序。"""
    return {
        "sequence": [f"{c['method']} {c['path']}" for c in STATE.calls],
        "calls": STATE.calls,
        "messages": list(STATE.messages.values()),
        "patched_messages": STATE.patched_messages,
        "notifications": [_effective(n) for n in STATE.notifications.values()],
        "attachments": [
            {
                "id": k,
                "filename": v["filename"],
                "size": len(v["content"]),
                "content_type": v["content_type"],
                "source_url": v.get("source_url"),
            }
            for k, v in STATE.attachment_blobs.items()
        ],
        "groups": list(STATE.groups.values()),
        "gap_alerts": list(STATE.gap_alerts.values()),
        "stats": STATE.stats,
        "digest_logs": STATE.digest_logs,
        "state_keys": [f"{ns}/{key}" for (ns, key) in STATE.kv],
        "blob_hits": STATE.blob_hits,
        "post_message_calls": STATE.post_message_calls,
    }


@app.post("/api/_fake/reset")
async def fake_reset():
    STATE.reset()
    return {"ok": True}


def main(argv: list[str] | None = None) -> int:
    import uvicorn

    parser = argparse.ArgumentParser(prog="python -m app.tools.fake_backend")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--fail-first", type=int, default=0, help="前 N 次 POST /api/messages 返回 500"
    )
    parser.add_argument("--fail-attachments", action="store_true", help="附件上传一律返回 500")
    parser.add_argument("--api-token", default="", help="非空则校验 Authorization: Bearer")
    parser.add_argument(
        "--media-max-bytes", type=int, default=DEFAULT_MEDIA_MAX_BYTES, help="附件大小上限"
    )
    args = parser.parse_args(argv)

    STATE.fail_first = args.fail_first
    STATE.fail_attachments = args.fail_attachments
    STATE.token = args.api_token
    STATE.media_max_bytes = args.media_max_bytes
    if args.fail_first:
        logger.info("已启用故障注入：前 %d 次 POST /api/messages 返回 500", args.fail_first)
    if args.api_token:
        logger.info("已启用令牌校验")
    logger.info("假后端监听 http://%s:%d", args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
