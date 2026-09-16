"""本地假后端：用来在没有真 backend 的情况下验证 bot。

    python -m app.tools.fake_backend --port 9000
    python -m app.tools.fake_backend --port 9000 --fail-first 2   # 前 2 次 /api/ingest/messages 返回 500

它实现的是**最小可用的后端契约**：
  - POST /api/ingest/messages                    （把收到的消息打印出来）
  - POST /api/tasks/manual                       （简陋规则解析，够跑通 /add 的两种分支）
  - GET  /api/notifications                      （固定 3 条演示数据）
  - POST /api/notifications/{id}/corrections     （只做参数校验）

`--fail-first 2` 是专门为重试逻辑准备的：配上它跑
`python -m app.tools.send_test --backend-url http://127.0.0.1:9000`，
就能在日志里看到 1s/2s 的退避重试，最后仍然成功。
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
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

# 只认几种最常见的说法，够演示用。真解析在 backend 的 pipeline/timeparse.py。
_WEEKDAYS = "一二三四五六日天"
_DUE_RE = re.compile(
    r"(今天|明天|后天|本周[" + _WEEKDAYS + r"]|下周[" + _WEEKDAYS + r"]|"
    r"周[" + _WEEKDAYS + r"]|\d{1,2}月\d{1,2}日|\d{4}-\d{2}-\d{2})"
    r"(上午|下午|晚上|中午)?(\d{1,2})?[点时:]?(\d{1,2})?分?"
)
_LOCATION_RE = re.compile(r"(教[一二三四五六七八九十\d]+\s?\d{0,4}|学工办|教务处|班长那里|线上)")


class IngestBody(BaseModel):
    messages: list[dict]


class ManualBody(BaseModel):
    text: str
    sender_id: str = ""
    sender_name: str = ""
    auto_commit: bool = True
    force_commit: bool = False


class CorrectionBody(BaseModel):
    field: str
    value: object | None = None
    user_id: str = "web"


class State:
    def __init__(self, fail_first: int = 0):
        self.fail_first = fail_first
        self.ingest_calls = 0
        self.received: list[dict] = []
        self.created: list[dict] = []


STATE = State()
app = FastAPI(title="Xcollector fake backend")


def _guess_due(text: str) -> tuple[int | None, str | None, float]:
    """返回 (due_at, due_text, confidence)。解析不出就 None。"""
    match = _DUE_RE.search(text)
    if not match:
        return None, None, 0.0

    day_text, period, hour_text, minute_text = match.groups()
    now = datetime.now(TZ)
    try:
        if day_text == "今天":
            day = now
        elif day_text == "明天":
            day = now + timedelta(days=1)
        elif day_text == "后天":
            day = now + timedelta(days=2)
        elif day_text and day_text.startswith(("本周", "下周", "周")):
            target = _WEEKDAYS.index(day_text[-1]) + 1  # 周一=1 ... 周日=7
            weekday = now.isoweekday()
            delta = (target - weekday) % 7
            if day_text.startswith("下周"):
                delta += 7 if delta else 7
            elif delta == 0:
                delta = 7  # "周三"说在周三当天，视为下周三（已过）
            day = now + timedelta(days=delta)
        elif day_text and "月" in day_text:
            month, day_of_month = re.findall(r"\d+", day_text)
            day = now.replace(month=int(month), day=int(day_of_month))
        else:
            day = now + timedelta(days=3)
    except Exception:
        return None, None, 0.0

    hour, minute = 23, 59
    if hour_text:
        hour = int(hour_text)
        minute = int(minute_text or 0)
        if period in ("下午", "晚上") and hour < 12:
            hour += 12
        elif period == "中午" and hour < 12:
            hour = 12
    due_at = int(day.replace(hour=hour, minute=minute, second=0, microsecond=0).timestamp() * 1000)
    return due_at, match.group(0), 0.8


def _guess_title(text: str) -> str:
    title = _DUE_RE.sub("", text)
    title = re.sub(r"^(把|请|要|得|在|于)\s*", "", title.strip())
    title = re.sub(r"(前|之前|以前)\s*", "", title)
    return title.strip()[:30] or text.strip()[:30]


def _new_id() -> str:
    return f"{int(time.time() * 1000):013d}{random.randint(0, 0xFFFF):04x}"


@app.post("/api/ingest/messages")
async def ingest(body: IngestBody):
    STATE.ingest_calls += 1
    if STATE.ingest_calls <= STATE.fail_first:
        logger.warning(
            "第 %d 次 ingest 调用 → 故意返回 500（--fail-first %d）",
            STATE.ingest_calls,
            STATE.fail_first,
        )
        return JSONResponse(status_code=500, content={"detail": "模拟后端故障"})

    STATE.received.extend(body.messages)
    logger.info("收到 %d 条消息（累计 %d 条）", len(body.messages), len(STATE.received))
    for msg in body.messages:
        logger.info(
            "  group=%s(%s) sender=%s ts=%s at_all=%s text=%s",
            msg.get("group_id"),
            msg.get("group_name"),
            msg.get("sender_name"),
            msg.get("ts"),
            msg.get("at_all"),
            (msg.get("text") or "").replace("\n", " / ")[:80],
        )
    return {"ok": True, "count": len(body.messages)}


@app.post("/api/tasks/manual")
async def manual(body: ManualBody):
    due_at, due_text, confidence = _guess_due(body.text)
    title = _guess_title(body.text)
    location_match = _LOCATION_RE.search(body.text)
    location = location_match.group(0) if location_match else None
    preview = {
        "title": title,
        "due_at": due_at,
        "due_text": due_text,
        "location": location,
        "summary": body.text,
        "due_confidence": confidence,
    }

    if due_at is None and not body.force_commit:
        logger.info("解析不出截止时间，要求确认：%s", body.text)
        return {"ok": True, "needs_confirm": True, "preview": preview, "task": None}

    task = {
        "id": _new_id(),
        "title": title,
        "due_at": due_at,
        "due_text": due_text,
        "location": location,
        "status": "active",
        "source": "manual",
        "sender_id": body.sender_id,
        "sender_name": body.sender_name,
    }
    STATE.created.append(task)
    logger.info("已建任务 %s（%s）", task["id"], title)
    return {"ok": True, "needs_confirm": False, "preview": preview, "task": task}


DEMO_NOTIFICATIONS = [
    {
        "id": "1757692800000aaaa",
        "title": "交实验报告",
        "due_at": None,  # 由下面的 handler 按"今天 15:00"动态填
        "due_text": "明天下午3点",
        "due_confidence": 0.9,
        "location": "教三201",
        "status": "active",
    },
    {
        "id": "1757692800000bbbb",
        "title": "提交军训心得",
        "due_at_offset_days": 7,
        "due_text": "下周三前",
        "due_confidence": 0.7,
        "location": None,
        "status": "active",
    },
    {
        "id": "1757692800000cccc",
        "title": "安全教育平台学习",
        "due_at_offset_days": 4,
        "due_text": None,
        "due_confidence": 0.0,
        "location": None,
        "status": "active",
    },
]


@app.get("/api/notifications")
async def notifications(
    status: str = Query(default="all"), limit: int = Query(default=50)
):
    now = datetime.now(TZ)
    out = []
    for item in DEMO_NOTIFICATIONS:
        row = dict(item)
        offset = row.pop("due_at_offset_days", None)
        if offset is not None:
            row["due_at"] = int(
                (now + timedelta(days=offset))
                .replace(hour=23, minute=59, second=0, microsecond=0)
                .timestamp()
                * 1000
            )
        elif row.get("due_at") is None and item["id"].endswith("aaaa"):
            row["due_at"] = int(
                now.replace(hour=15, minute=0, second=0, microsecond=0).timestamp() * 1000
            )
        if status not in ("all", "") and row["status"] != status:
            continue
        out.append(row)
    return {"notifications": out[:limit], "server_time": int(time.time() * 1000)}


@app.post("/api/notifications/{notif_id}/corrections")
async def corrections(notif_id: str, body: CorrectionBody):
    if body.field == "status" and body.value not in ("active", "archived", "done"):
        # 注意：真 backend 目前只接受 active/archived。
        # 这里放宽是为了让 /done 的自检能跑完；接入真后端前必须确认它已支持 done。
        raise HTTPException(status_code=400, detail=f"status 非法：{body.value}")
    logger.info(
        "修正 notif=%s field=%s value=%s by=%s",
        notif_id,
        body.field,
        body.value,
        body.user_id,
    )
    return {"ok": True, "notification": {"id": notif_id, "status": body.value}}


@app.get("/api/_fake/state")
async def fake_state():
    """自检用：看假后端到底收到了什么。"""
    return {
        "ingest_calls": STATE.ingest_calls,
        "received_count": len(STATE.received),
        "received": STATE.received,
        "created_tasks": STATE.created,
    }


def main(argv: list[str] | None = None) -> int:
    import uvicorn

    parser = argparse.ArgumentParser(prog="python -m app.tools.fake_backend")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--fail-first", type=int, default=0, help="前 N 次 /api/ingest/messages 返回 500"
    )
    args = parser.parse_args(argv)

    STATE.fail_first = args.fail_first
    if args.fail_first:
        logger.info("已启用故障注入：前 %d 次 ingest 返回 500", args.fail_first)
    logger.info("假后端监听 http://%s:%d", args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
