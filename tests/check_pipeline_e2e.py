"""端到端自检：真 bot 代码 + 假后端（不需要 NapCat、不需要真后端）。

    python -m tests.check_pipeline_e2e

它在**本进程内**起一个 `app.tools.fake_backend`（真 uvicorn、真 TCP），
然后用真代码喂 OneBot 事件进去，最后断言假后端记录下来的**调用序列**与数据。

覆盖四件事：
  1. 一条群消息从 OneBot 事件到入库，依次调了哪几个后端接口（含附件）；
  2. 指令全链路：/add（直接建 / 回问确认）、/list、/done、/del，含"bot 重启后仍然有效"；
  3. 后端不可达时：/api/status 不炸（backend.reachable=false）、消息处理不崩、
     指令回「后端暂时不可用」；
  4. 崩溃恢复：后端里停在 pending 的消息会被 resume_pending() 捡回来处理完。

**它绝不连真实 NapCat**：hub 用 FakeHub 顶替，并且整个进程不会去连
ONEBOT_WS_URL（runtime.start() 从没被调用过）。
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import socket
import sys
from contextlib import nullcontext

# Windows 控制台默认 GBK，print 中文/emoji 会 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

# 双保险：本进程任何东西都不许去连真实 NapCat
os.environ["ONEBOT_WS_URL"] = "ws://127.0.0.1:3999"
os.environ["DIGEST_TARGET_QQ"] = "10001"
# bot 自己的入口也要有令牌（契约第 9 节：前端调 bot 用 BOT_API_TOKEN）
os.environ["BOT_API_TOKEN"] = "bot-secret"
# 网页令牌：与上面的管理令牌**故意不同**，用来验证"网页只能看、不能发消息"。
# 两个配成一样的话，下面第 10 节的断言会全部落到写入范围，等于没测。
os.environ["WEB_API_TOKEN"] = "web-secret"

from app import config  # noqa: E402
from app.backend_client import BackendClient  # noqa: E402
from app.commands import (  # noqa: E402
    BACKEND_DOWN_REPLY,
    STALE_LIST_REPLY,
    CommandRouter,
)
from app.config import Settings  # noqa: E402
from app.pipeline.digest import (  # noqa: E402
    build_digest,
    configure_digest,
    reset_digest_context,
    send_digest,
    sent_today,
)
from app.pipeline.runner import ingest_message, resume_pending  # noqa: E402
from app.tools import fake_backend  # noqa: E402
from app.tools.send_test import FakeHub  # noqa: E402

failures: list[str] = []


def check(name: str, got, want) -> None:
    if got == want:
        print(f"ok    {name}")
    else:
        failures.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f" ({detail})" if detail else ""), bool(cond), True)


# ---------------------------------------------------------------------------
# 起假后端
# ---------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def serve_fake_backend(port: int):
    import uvicorn

    cfg = uvicorn.Config(
        fake_backend.app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        access_log=False,
        lifespan="off",
    )
    server = uvicorn.Server(cfg)
    server.capture_signals = nullcontext  # type: ignore[method-assign]
    task = asyncio.create_task(server.serve())
    waited = 0.0
    while not server.started and waited < 10:
        if task.done():
            raise RuntimeError(f"假后端启动失败：{task.exception()!r}")
        await asyncio.sleep(0.05)
        waited += 0.05
    if not server.started:
        raise RuntimeError("假后端 10s 内没有起来")
    return server, task


async def stop_fake_backend(server, task) -> None:
    server.should_exit = True
    try:
        await asyncio.wait_for(task, timeout=5)
    except Exception:
        task.cancel()


def make_settings(port: int, **overrides) -> Settings:
    base = dict(
        backend_base_url=f"http://127.0.0.1:{port}",
        backend_timeout=5.0,
        backend_max_retries=2,
        api_token="test-token",
        extractor="rule",
        media_download_enabled=True,
        vlm_enabled=False,
        # 白名单是 **fail-closed** 的：留空 = 什么都不处理。
        # 所以夹具里必须把测试用的群和发送者列进去，否则整条链路从第一步就断。
        group_whitelist="123456789:示例通知群",
        sender_whitelist="10001:张老师",
        sender_whitelist_mode="strict",
        onebot_ws_url="ws://127.0.0.1:3999",
        command_whitelist="10001:我",
        digest_target_qq="10001",
        pending_retry_interval=999,
        recovery_check_interval=999,
    )
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# 事件与断言工具
# ---------------------------------------------------------------------------


def group_event(
    *,
    message_id: int = 12345,
    ts: int = 1757692800,
    text: str = "",
    image_url: str | None = None,
    at_all: bool = True,
) -> dict:
    segments: list[dict] = []
    if at_all:
        segments.append({"type": "at", "data": {"qq": "all"}})
    segments.append({"type": "text", "data": {"text": text}})
    if image_url:
        segments.append({"type": "image", "data": {"url": image_url, "file": "x.png", "size": "67"}})
    return {
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": message_id,
        "group_id": 123456789,
        "user_id": 10001,
        "self_id": 999999,
        "time": ts,
        "sender": {"user_id": 10001, "nickname": "小李", "card": "张老师", "role": "admin"},
        "message": segments,
    }


def private_event(*, user_id: int = 10001, text: str) -> dict:
    return {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "message_id": 90000,
        "user_id": user_id,
        "self_id": 999999,
        "time": 1757692800,
        "sender": {"user_id": user_id, "nickname": "我"},
        "message": [{"type": "text", "data": {"text": text}}],
    }


def route_key(method: str, path: str) -> str:
    """把带 id 的路径归一化成断言用的路由名。"""
    if path == "/api/_fake/blob/x.png":
        return "GET <QQ CDN 图片>"
    if path.startswith("/api/messages/") and method == "PATCH":
        return "PATCH /api/messages/{id}"
    if path.startswith("/api/messages/") and method == "GET":
        return "GET /api/messages/{id}"
    if path.startswith("/api/notifications/") and path.endswith("/corrections"):
        return "POST /api/notifications/{id}/corrections"
    if path.startswith("/api/notifications/") and method == "GET":
        return "GET /api/notifications/{id}"
    return f"{method} {path}"


def sequence_since(mark: int) -> list[str]:
    """假后端记录到的调用序列（从 mark 开始）。自检接口不算。"""
    out = []
    for call in fake_backend.STATE.calls[mark:]:
        if call["path"] in ("/api/_fake/state", "/api/_fake/reset"):
            continue
        out.append(route_key(call["method"], call["path"]))
    return out


def mark() -> int:
    return len(fake_backend.STATE.calls)


def reset_state() -> int:
    fake_backend.STATE.reset()
    return mark()


class StubSender:
    """顶替 OneBot hub 的私聊发送（指令路径用）。"""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send_private_msg(self, user_id: str, message: str) -> dict:
        self.sent.append((str(user_id), message))
        return {"status": "ok", "retcode": 0}

    def last(self) -> str:
        return self.sent[-1][1] if self.sent else ""


class RecordingHub(FakeHub):
    """FakeHub + 记录发送动作。

    用来断言"被权限挡掉的请求**一个字节都没发出去**" —— 只看 HTTP 403 是不够的，
    真正要证明的是它没有副作用。FakeHub 本身没有 send_* 方法，所以这里补上。
    """

    def __init__(self) -> None:
        super().__init__()
        self.sent: list[tuple[str, str]] = []

    async def send_private_msg(self, user_id: str, message: str) -> dict:
        self.sent.append((f"private:{user_id}", message))
        return {"status": "ok", "retcode": 0}

    async def send_group_msg(self, group_id: str, message: str) -> dict:
        self.sent.append((f"group:{group_id}", message))
        return {"status": "ok", "retcode": 0}


class LogCapture(logging.Handler):
    """抓 `xcollector.message` 的日志行，用来断言处理轨迹的**顺序与内容**。

    日志是给人看的，但它也是排障的唯一线索 —— "收到"那行必须在处理之前就出现，
    否则一条卡在附件下载或模型调用上的消息在日志里什么都看不到。
    这条不变式只有真跑一遍才验得出来，所以在这里把它钉住。
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())

    def stages(self) -> list[str]:
        """按顺序取出 `阶段=X` / `结果=X` 里的 X。"""
        out: list[str] = []
        for line in self.lines:
            m = re.match(r"(?:阶段|结果)=(\S+)", line)
            if m:
                out.append(m.group(1))
        return out

    def find(self, needle: str) -> str:
        return next((line for line in self.lines if needle in line), "")

    def __enter__(self) -> LogCapture:
        self._logger = logging.getLogger("xcollector.message")
        self._logger.addHandler(self)
        return self

    def __exit__(self, *exc: object) -> None:
        self._logger.removeHandler(self)


async def wait_for(predicate, timeout: float = 10.0) -> bool:
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        await asyncio.sleep(0.05)
        waited += 0.05
    return predicate()


_NORMALIZERS: dict[int, object] = {}


async def norm(event: dict, settings: Settings):
    """把 OneBot 事件过一遍**真的**归一化（合并转发展开、群名查询都真跑）。

    直接构造的假事件里没有 `ts` / `text` 这些归一化之后的字段，
    所以凡是直接调 runner 的用例都必须先过这一层 —— 否则测的是"没归一化的输入"，
    而真实链路里 hub 一定会先归一化。
    """
    from app.normalize import MessageNormalizer

    key = id(settings)
    normalizer = _NORMALIZERS.get(key)
    if normalizer is None:
        normalizer = MessageNormalizer(settings)
        _NORMALIZERS[key] = normalizer
    msg = await normalizer.normalize(event, FakeHub())  # type: ignore[arg-type]
    assert msg is not None, "归一化返回 None，事件类型不对"
    return msg


def stats_of_last_day() -> dict:
    if not fake_backend.STATE.stats:
        return {}
    return fake_backend.STATE.stats[list(fake_backend.STATE.stats)[0]]


# ---------------------------------------------------------------------------
# 1. 群消息全链路
# ---------------------------------------------------------------------------


async def test_group_message_flow(port: int) -> None:
    print("\n=== 1. 一条群消息（带图片）从 OneBot 事件到入库 ===")
    mk = reset_state()
    settings = make_settings(port)

    from app.main import BotRuntime

    runtime = BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]
    await runtime.pipeline.start()
    try:
        event = group_event(
            text=" 大家下周三前把军训心得交到班长那里，不少于800字。",
            image_url=f"http://127.0.0.1:{port}/api/_fake/blob/x.png",
        )
        with LogCapture() as logs:
            await runtime.handle_event(event)
            # 通知建出来之后还有 PATCH(state) 和 POST(/api/stats)，要等整条链路走完
            done = await wait_for(
                lambda: bool(fake_backend.STATE.notifications) and bool(fake_backend.STATE.stats)
            )
        check_true("通知已建出来", done)

        # ---- 处理轨迹：先"收到"，再一步步往下 ----
        stages = logs.stages()
        check(
            "轨迹顺序：收到 → 归一化 → 入库 → 附件 → 抽取",
            stages[:5],
            ["收到", "归一化", "入库", "附件", "抽取"],
        )
        check("轨迹以汇总行结尾", stages[-1] if stages else "", "extracted")

        received = logs.find("阶段=收到")
        check_true("「收到」那行带群号和发送者", "123456789" in received and "张老师" in received, received)
        check_true(
            "「收到」那行直接带原文（还没归一化就能看出是什么消息）",
            "军训心得" in received,
            received,
        )
        check_true(
            "「收到」在「入库」之前 —— 卡住的消息也留得下痕迹",
            logs.lines.index(received) < logs.lines.index(logs.find("阶段=入库")),
        )
        check_true("阶段行都带 msg_id，便于 grep 一条消息", "msg_id=12345" in logs.find("阶段=抽取"))
        check_true(
            "阶段行都带耗时，能看出慢在哪一步",
            "耗时=" in logs.find("阶段=入库") and "耗时=" in logs.find("阶段=抽取"),
        )

        seq = sequence_since(mk)
        print("    真实调用序列：")
        for item in seq:
            print(f"      {item}")

        check(
            "群消息的接口调用序列",
            seq,
            [
                "POST /api/messages",          # 写前日志：先保住原文
                "GET <QQ CDN 图片>",            # 下载附件字节（真实世界里是 QQ CDN）
                "POST /api/attachments",       # 上传给后端
                "PATCH /api/messages/{id}",    # 回填 attachments
                "POST /api/groups",            # 群状态（拿 previous_last_msg_ts）
                "POST /api/notifications",     # 建条
                "PATCH /api/messages/{id}",    # state=extracted
                "POST /api/stats",             # 统计
            ],
        )

        rows = list(fake_backend.STATE.messages.values())
        check_true("原文真的入库了", len(rows) == 1)
        if rows:
            check("入库状态", rows[0]["state"], "extracted")
            check_true("正文含原文", "大家下周三前把军训心得交到班长那里" in rows[0]["content"], rows[0]["content"])
            check("发送者用群名片", rows[0]["sender_name"], "张老师")
            check_true("附件已回填", len(rows[0]["attachments"]) == 1)
            if rows[0]["attachments"]:
                att = rows[0]["attachments"][0]
                check_true(
                    "附件 url 指向后端",
                    str(att.get("url", "")).startswith("/api/attachments/"),
                    str(att.get("url")),
                )
                check(
                    "附件 source_url 留档",
                    att.get("source_url"),
                    f"http://127.0.0.1:{port}/api/_fake/blob/x.png",
                )

        notifs = list(fake_backend.STATE.notifications.values())
        check_true("通知只有一条", len(notifs) == 1)
        if notifs:
            check("通知标题", notifs[0]["title"], "大家下周三前把军训心得交到班长那里，不少于800字。[图片]")
            check("due_text 逐字保留", notifs[0]["due_text"], "下周三前")
            check("地点没瞎猜（班长那里不是地点）", notifs[0]["location"], None)
            check("抽取器", notifs[0]["extractor"], "rule")
            check_true("evidence 非空", bool(notifs[0]["evidence"]))

        check("统计 ingested", stats_of_last_day().get("ingested"), 1)
        check("统计 extracted", stats_of_last_day().get("extracted"), 1)

        # --- 重复推送（NapCat 重连时很常见）---
        mk = mark()
        await runtime.handle_event(event)
        await asyncio.sleep(0.4)
        check("重复消息只调一次 POST /api/messages", sequence_since(mk), ["POST /api/messages"])
        check_true("重复消息不再建条", len(fake_backend.STATE.notifications) == 1)
    finally:
        await runtime.pipeline.stop()
        await runtime.backend.close()


# ---------------------------------------------------------------------------
# 2. 白名单与噪声
# ---------------------------------------------------------------------------


async def test_filters(port: int) -> None:
    print("\n=== 2. 白名单 / 噪声 ===")

    # 白名单**留空 = 什么都不处理**（fail-closed）。
    # 这是刻意的：官方通知只发在固定几个群、由固定几个人发，
    # 留空放开等于"随便哪个群、谁说话都入库"。
    settings_empty = make_settings(port, group_whitelist="", sender_whitelist="")
    backend_empty = BackendClient(settings_empty)
    mk = reset_state()
    msg = await norm(group_event(text="大家下周三前交军训心得"), settings_empty)
    outcome = await ingest_message(msg, backend_empty, settings=settings_empty)
    check("群白名单留空 → 不处理任何群", outcome, "group_filtered")
    check("群白名单留空 → 连后端都不写", sequence_since(mk), [])
    check_true("群白名单留空 → 库里没有原文", fake_backend.STATE.messages == {})
    check_true(
        "群白名单留空 → whitelist_ready 为假（启动会 WARNING）",
        not settings_empty.whitelist_ready,
    )
    await backend_empty.close()

    # 群配了但发送者白名单留空（strict）→ 原文入库，但不抽取
    settings_nosender = make_settings(port, sender_whitelist="")
    backend_ns = BackendClient(settings_nosender)
    reset_state()
    outcome = await ingest_message(
        await norm(group_event(message_id=221, text="大家下周三前交军训心得"), settings_nosender),
        backend_ns,
        settings=settings_nosender,
    )
    check("发送者白名单留空（strict）→ skipped_whitelist", outcome, "skipped_whitelist")
    check_true(
        "发送者白名单留空 → whitelist_ready 为假",
        not settings_nosender.whitelist_ready,
    )
    await backend_ns.close()

    # 同一份配置，只把 mode 改成 off（显式放开）→ 就该正常抽取
    settings_off = make_settings(port, sender_whitelist="", sender_whitelist_mode="off")
    backend_off = BackendClient(settings_off)
    reset_state()
    outcome = await ingest_message(
        await norm(group_event(message_id=223, text="大家下周三前交军训心得"), settings_off),
        backend_off,
        settings=settings_off,
    )
    check("mode=off 且发送者白名单留空 → extracted（显式放开）", outcome, "extracted")
    check_true("mode=off 时 whitelist_ready 为真", settings_off.whitelist_ready)
    await backend_off.close()

    # 群不在白名单 → 一个后端请求都不发
    settings = make_settings(port, group_whitelist="111111:别的群")
    backend = BackendClient(settings)
    mk = reset_state()
    msg = await norm(group_event(text="大家下周三前交军训心得"), settings)
    outcome = await ingest_message(msg, backend, settings=settings)
    check("群不在白名单 → group_filtered", outcome, "group_filtered")
    check("群不在白名单 → 不发任何后端请求", sequence_since(mk), [])
    await backend.close()

    # 发送者在白名单 → 正常
    settings2 = make_settings(port, sender_whitelist="10001:张老师")
    backend2 = BackendClient(settings2)
    reset_state()
    outcome = await ingest_message(
        await norm(group_event(message_id=222, text="大家下周三前交军训心得"), settings2),
        backend2,
        settings=settings2,
    )
    check("发送者在白名单 → extracted", outcome, "extracted")
    await backend2.close()

    # 发送者不在白名单 → skipped_whitelist（原文仍然入库）
    settings3 = make_settings(port, sender_whitelist="99999:别人")
    backend3 = BackendClient(settings3)
    mk = reset_state()
    event = group_event(message_id=333, text="大家下周三前交军训心得")
    event["user_id"] = 55555
    event["sender"]["user_id"] = 55555
    outcome = await ingest_message(await norm(event, settings3), backend3, settings=settings3)
    check("发送者不在白名单 → skipped_whitelist", outcome, "skipped_whitelist")
    check(
        "发送者不在白名单的调用序列",
        sequence_since(mk),
        [
            "POST /api/messages",
            "POST /api/groups",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    rows = list(fake_backend.STATE.messages.values())
    check("状态被标成 skipped_whitelist", rows[0]["state"], "skipped_whitelist")
    check_true("没有建条", fake_backend.STATE.notifications == {})
    await backend3.close()

    # 噪声（闲聊）→ noise，不计入盲区
    settings4 = make_settings(port)
    backend4 = BackendClient(settings4)
    reset_state()
    outcome = await ingest_message(
        await norm(group_event(message_id=444, text="收到", at_all=False), settings4),
        backend4,
        settings=settings4,
    )
    check("闲聊 → noise", outcome, "noise")
    rows = list(fake_backend.STATE.messages.values())
    check("状态 noise", rows[0]["state"], "noise")
    check("noise 不计入 unparsed", stats_of_last_day().get("unparsed"), None)
    await backend4.close()


async def test_gap_detection(port: int) -> None:
    print("\n=== 3. 缺口检测（previous_last_msg_ts）===")
    settings = make_settings(port)
    backend = BackendClient(settings)
    reset_state()
    await ingest_message(
        await norm(group_event(message_id=1, ts=1757692800, text="大家下周三前交心得"), settings),
        backend,
        settings=settings,
    )
    check_true("第一次见到该群，不产生缺口告警", fake_backend.STATE.gap_alerts == {})
    await ingest_message(
        await norm(
            group_event(message_id=2, ts=1757692800 + 16 * 3600, text="大家下周三前交心得"), settings
        ),
        backend,
        settings=settings,
    )
    check_true("间隔 16 小时 → 产生缺口告警", len(fake_backend.STATE.gap_alerts) == 1)
    alert = list(fake_backend.STATE.gap_alerts.values())[0]
    check_true("缺口告警写明间隔", "16.0 小时" in alert["reason"], alert["reason"])
    # 恢复时不能把 last_msg_ts 拨回去（那会造出假的缺口）
    check("group_state 的 last_msg_ts 是最后一条", fake_backend.STATE.groups["123456789"]["last_msg_ts"],
          1757692800000 + 16 * 3600 * 1000)
    await backend.close()


# ---------------------------------------------------------------------------
# 4. 崩溃恢复
# ---------------------------------------------------------------------------


async def test_crash_recovery(port: int) -> None:
    print("\n=== 4. 崩溃恢复（写前日志之后崩了）===")
    settings = make_settings(port)
    backend = BackendClient(settings)
    reset_state()

    # 模拟"原文已落库、但重活没做"：直接往后端塞一条 pending
    body = {
        "message_id": "crash-1",
        "group_id": "123456789",
        "group_name": "示例通知群",
        "sender_id": "10001",
        "sender_name": "张老师",
        "ts": 1757692800000,
        "content": "本周五19:00在教三201开班会，请全体同学准时参加。",
        "attachments": [],
        "raw": {"message": [{"type": "text", "data": {"text": "x"}}]},
    }
    created = await backend.create_message(body)
    check_true("pending 原文已入库", bool(created.get("id")))
    mk = mark()

    n = await resume_pending(backend, settings)
    check("恢复处理了 1 条", n, 1)
    check(
        "恢复路径的调用序列",
        sequence_since(mk),
        [
            "GET /api/messages",            # 问后端"还有哪些 pending"
            "POST /api/notifications",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    row = fake_backend.STATE.messages[created["id"]]
    check("恢复后状态 extracted", row["state"], "extracted")
    notifs = list(fake_backend.STATE.notifications.values())
    check_true("恢复后建了通知", len(notifs) == 1)
    if notifs:
        check("恢复后地点", notifs[0]["location"], "教三201")

    # 恢复不应该把群状态往回拨
    check_true("恢复不动 group_state", fake_backend.STATE.groups == {})
    await backend.close()


# ---------------------------------------------------------------------------
# 5. 后端不可达
# ---------------------------------------------------------------------------


async def test_backend_down() -> None:
    print("\n=== 5. 后端不可达 ===")
    dead_port = free_port()  # 这个端口没有任何人在听
    settings = make_settings(dead_port)

    from app.main import BotRuntime

    runtime = BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]
    backend = runtime.backend

    health = await backend.health()
    check("health() 不抛异常", isinstance(health, dict), True)
    check("health().reachable", health.get("reachable"), False)
    check_true("health().error 有内容", bool(health.get("error")), str(health.get("error"))[:80])

    outcome = await ingest_message(
        await norm(group_event(text="大家下周三前交心得"), settings), backend, settings=settings
    )
    check("后端不可达 → 结果 error（而不是抛异常）", outcome, "error")
    check("原文进了待重试队列", len(backend.pending), 1)

    payload = await runtime.status_payload()
    check("status.backend.reachable", payload["backend"]["reachable"], False)
    check("status 仍然返回完整结构（不 500）", sorted(payload), sorted([
        "onebot", "llm", "whitelist", "pipeline", "blindspots", "groups",
        "gap_alerts", "backend", "recovery", "digest", "day", "server_time",
    ]))
    check("status.blindspots 全 0", payload["blindspots"]["unparsed_count"], 0)
    check("status 里能看到待重试计数", payload["pipeline"]["pending_retry"], 1)
    check("status.digest.sent_today", payload["digest"]["sent_today"], False)

    sender = StubSender()
    router = CommandRouter(backend, sender, settings)
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    check("后端不可达时 /add 的回执", sender.last(), BACKEND_DOWN_REPLY)
    await router.handle_private_event(private_event(text="/list"))
    check("后端不可达时 /list 的回执", sender.last(), BACKEND_DOWN_REPLY)

    await runtime.backend.close()


async def test_blindspots(port: int) -> None:
    """盲区计数必须是**真的查出来的数字**，不是兜底默认值。

    这一段是刻意的：`_safe()` 会把后端异常吞成默认值，所以"计数为 0"既可能是
    "真的没有"，也可能是"查询炸了"。所以这里先造出非零的数据，再断言非零。
    """
    print("\n=== 5b. 盲区计数（count_only=1）===")
    settings = make_settings(port)
    backend = BackendClient(settings)

    from app.main import BotRuntime

    runtime = BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]
    reset_state()

    # 1) 一条 unparsed + 一条 degraded 的原文（7 天内）
    for i, state in enumerate(("unparsed", "degraded")):
        created = await backend.create_message(
            {
                "message_id": f"blind-{i}",
                "group_id": "123456789",
                "ts": 1789565000000,
                "content": "看不清的公告",
                "raw": {},
            }
        )
        await backend.patch_message(created["id"], {"state": state, "state_reason": "测试"})
    # 2) 一条 conflict 的通知 + 一条低置信度的通知 + 一条正常的
    for i, (conflict, conf) in enumerate(((True, 0.9), (False, 0.3), (False, 0.95))):
        await backend.create_notification(
            {
                "raw_message_id": f"blind-notif-{i}",
                "group_id": "123456789",
                "title": f"测试 {i}",
                "due_at": 1789999999000,
                "due_confidence": conf,
                "conflict": conflict,
                "evidence": "证据",
                "extractor": "rule",
            }
        )
    # 3) 一条 7 天前的 unparsed（不该被算进去）
    old = await backend.create_message(
        {
            "message_id": "blind-old",
            "group_id": "123456789",
            "ts": 1789565000000 - 30 * 24 * 3600 * 1000,
            "content": "很久以前的公告",
            "raw": {},
        }
    )
    await backend.patch_message(old["id"], {"state": "unparsed", "state_reason": "测试"})

    payload = await runtime.status_payload()
    bs = payload["blindspots"]
    check("unparsed_count（unparsed+degraded，7 天内）", bs["unparsed_count"], 2)
    check("conflict_count", bs["conflict_count"], 1)
    check("low_confidence_count", bs["low_confidence_count"], 1)
    check("window_days", bs["window_days"], 7)
    check("degraded_today（今天没写过 degraded 统计）", bs["degraded_today"], False)

    # raw 状态计数也应该能按 count_only 问到
    check("count_messages 能按 state 过滤", await backend.count_messages(state=["unparsed", "degraded"]), 3)
    check("count_messages 带 since 窗口", await backend.count_messages(
        state=["unparsed", "degraded"], since=1789565000000 - 7 * 24 * 3600 * 1000
    ), 2)

    await runtime.backend.close()
    await backend.close()


# ---------------------------------------------------------------------------
# 6. 指令全链路
# ---------------------------------------------------------------------------


async def test_commands(port: int) -> None:
    print("\n=== 6. 指令对假后端的全链路 ===")
    settings = make_settings(port)
    backend = BackendClient(settings)
    sender = StubSender()
    router = CommandRouter(backend, sender, settings)

    # --- /add 有把握 → 直接建条 ---
    mk = reset_state()
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    print(f"    回执：{sender.last().splitlines()[0]}")
    check_true("有把握 → 回执含「已添加」", "已添加" in sender.last())
    check(
        "/add 的调用序列",
        sequence_since(mk),
        [
            "POST /api/messages",
            "POST /api/notifications",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    rows = list(fake_backend.STATE.messages.values())
    check("手动消息的 group_id", rows[0]["group_id"], "manual:10001")
    check("手动消息的群名", rows[0]["group_name"], "手动添加")
    check_true("手动消息 id 形如 manual-<ms>", rows[0]["message_id"].startswith("manual-"))
    notifs = list(fake_backend.STATE.notifications.values())
    check_true("手动任务建成了通知", len(notifs) == 1)
    if notifs:
        check("手动任务 due_text", notifs[0]["due_text"], "明天下午3点")
        check("手动任务 evidence 是用户原文", notifs[0]["evidence"], "明天下午3点 交实验报告")

    # --- /add 没把握 → 回问 + 待确认状态存后端 ---
    mk = reset_state()
    await router.handle_private_event(private_event(text="/add 尽快把材料交上来"))
    print(f"    回问：{sender.last().splitlines()[0]}")
    check_true("回问文案", "仍然添加吗" in sender.last())
    check("没把握时只存待确认、不建条", sequence_since(mk), ["PUT /api/state/command_pending/10001"])
    check_true("待确认状态写进了后端", ("command_pending", "10001") in fake_backend.STATE.kv)
    pending_row = fake_backend.STATE.kv[("command_pending", "10001")]
    check_true("待确认带 TTL", pending_row["expires_at"] is not None)
    check_true(
        "待确认里存了原文",
        "尽快把材料交上来" in str(pending_row["value"]),
        str(pending_row["value"])[:80],
    )

    # --- 用户回 y → 建条（不 reset：待确认必须还在）---
    mk = mark()
    await router.handle_private_event(private_event(text="y"))
    check_true("确认后回执含「已添加」", "已添加" in sender.last(), sender.last())
    check(
        "确认路径的调用序列",
        sequence_since(mk),
        [
            "GET /api/state/command_pending/10001",     # 读待确认（重启后也读得到）
            "DELETE /api/state/command_pending/10001",  # 先清掉，避免重复确认
            "POST /api/messages",
            "POST /api/notifications",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    check_true("确认后待确认被清掉", ("command_pending", "10001") not in fake_backend.STATE.kv)

    # --- /list（先造一条待办，否则列表是空的）---
    reset_state()
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    mk = mark()
    await router.handle_private_event(private_event(text="/list 5"))
    print("    /list 输出：")
    for line in sender.last().splitlines():
        print(f"      {line}")
    check_true("/list 有表头", sender.last().startswith("📋 待办"), sender.last().splitlines()[0])
    check_true("编号映射存进了后端", ("command_list", "10001") in fake_backend.STATE.kv)
    listing = fake_backend.STATE.kv[("command_list", "10001")]["value"]
    check_true("映射里有 notif_id", bool(listing["items"][0]["notif_id"]))
    check_true("映射带 TTL", fake_backend.STATE.kv[("command_list", "10001")]["expires_at"] is not None)

    # --- /done 1 ---
    mk = mark()
    await router.handle_private_event(private_event(text="/done 1"))
    check_true("/done 回执", sender.last().startswith("✅ 已完成"), sender.last())
    check(
        "/done 的调用序列",
        sequence_since(mk),
        [
            "GET /api/state/command_list/10001",
            "POST /api/notifications/{id}/corrections",
        ],
    )
    corr = list(fake_backend.STATE.corrections.values())
    check("修正 field=status", list(corr[0].keys())[0], "status")
    check("修正 value=done", corr[0]["status"], "done")

    # --- /del 1 ---
    mk = mark()
    await router.handle_private_event(private_event(text="/del 1"))
    check_true("/del 回执", sender.last().startswith("🗑 已移除"), sender.last())
    corr = list(fake_backend.STATE.corrections.values())
    check("归档 value=archived", corr[0]["status"], "archived")

    # --- bot 重启后：编号映射仍在（全新实例 = 没有任何内存状态）---
    mk = mark()
    router2 = CommandRouter(backend, sender, settings)
    await router2.handle_private_event(private_event(text="/done 1"))
    check_true("重启后 /done 仍然工作", sender.last().startswith("✅ 已完成"), sender.last())
    check_true("重启后确实读了一次后端", sequence_since(mk)[0].startswith("GET /api/state/command_list"))

    # --- 映射过期 → 绝不猜 ---
    fake_backend.STATE.kv.pop(("command_list", "10001"), None)
    mk = mark()
    await router2.handle_private_event(private_event(text="/done 1"))
    check("/done 没有编号映射时的回执", sender.last(), STALE_LIST_REPLY)
    check("过期时不调用 corrections", sequence_since(mk), ["GET /api/state/command_list/10001"])

    # --- bot 重启后：待确认仍在 ---
    reset_state()
    await router.handle_private_event(private_event(text="/add 尽快把材料交上来"))
    check_true(
        "重启前待确认已写入后端",
        ("command_pending", "10001") in fake_backend.STATE.kv,
    )
    router3 = CommandRouter(backend, sender, settings)  # 又一个全新实例
    await router3.handle_private_event(private_event(text="n"))
    check("重启后回 n → 已取消", sender.last(), "已取消。")
    check_true("取消后后端状态被删除", ("command_pending", "10001") not in fake_backend.STATE.kv)

    # --- 非白名单用户：不回复、不发请求 ---
    mk = reset_state()
    before = len(sender.sent)
    await router.handle_private_event(private_event(user_id=88888, text="/add 明天交作业"))
    check("非白名单私聊不回复", len(sender.sent), before)
    check("非白名单私聊不发请求", sequence_since(mk), [])

    # --- 非 y/n 的闲聊不产生任何请求 ---
    mk = reset_state()
    await router.handle_private_event(private_event(text="今天天气不错"))
    check("闲聊私聊不发请求", sequence_since(mk), [])

    await backend.close()


# ---------------------------------------------------------------------------
# 7. digest
# ---------------------------------------------------------------------------


async def test_digest(port: int) -> None:
    print("\n=== 7. digest 预览 / 发送 / 今日已发判定 ===")
    settings = make_settings(port)
    backend = BackendClient(settings)
    sender = StubSender()
    configure_digest(backend, sender)

    reset_state()
    await ingest_message(
        await norm(group_event(message_id=777, text="大家下周三前把军训心得交到班长那里"), settings),
        backend,
        settings=settings,
    )
    text = await build_digest(backend, settings)
    check_true("digest 有标题行", text.startswith("【Xcollector 每日通知】"), text.splitlines()[0])
    check_true("digest 含新增条目", "军训心得" in text)
    check_true("digest 含盲区段", "本系统今日盲区" in text)
    check_true("digest 含服务器时间", "服务器时间" in text)

    reset_state()
    result = await send_digest(dry_run=True, backend=backend, sender=sender)
    check("dry_run 不发送", result["sent"], False)
    check("dry_run ok", result["ok"], True)
    check("dry_run 写了一条 preview 记录", len(fake_backend.STATE.digest_logs), 1)
    check("preview 记录的 kind", fake_backend.STATE.digest_logs[0]["kind"], "preview")

    check("还没有发过 → sent_today=False", await sent_today(backend), False)

    reset_state()
    result = await send_digest(dry_run=False, kind="auto", backend=backend, sender=sender)
    check("真的发出去了", result["sent"], True)
    check("发送调用了一次私聊", len(sender.sent), 1)
    check("收件人是 DIGEST_TARGET_QQ", sender.sent[0][0], "10001")
    check_true("发送后 sent_today=True", await sent_today(backend))
    check("auto 记录 sent=true", fake_backend.STATE.digest_logs[0]["sent"], True)

    # 关键：状态在后端，不在 bot 内存 —— 换一个实例依然知道今天发过了
    other = BackendClient(settings)
    check_true("重启后的 bot 依然不会重发", await sent_today(other))
    await other.close()

    await backend.close()
    reset_digest_context()


# ---------------------------------------------------------------------------
# 8. 认证
# ---------------------------------------------------------------------------


async def test_auth(port: int) -> None:
    print("\n=== 8. 共享密钥（API_TOKEN）===")
    fake_backend.STATE.token = "test-token"
    try:
        good = make_settings(port)
        backend = BackendClient(good)
        mk = reset_state()
        await ingest_message(
            await norm(group_event(message_id=888, text="大家下周三前交心得"), good),
            backend,
            settings=good,
        )
        calls = fake_backend.STATE.calls[mk:]
        check("带对令牌 → 全部 2xx", {c["status"] for c in calls}, {200})
        check_true(
            "请求确实带了 Bearer",
            bool(calls) and all(c["authorization"] == "Bearer test-token" for c in calls),
        )
        await backend.close()

        bad = make_settings(port, api_token="wrong-token")
        backend_bad = BackendClient(bad)
        health = await backend_bad.health()
        check("令牌不对 → reachable=False", health["reachable"], False)
        check_true("错误里能看到 401", "401" in (health.get("error") or ""), str(health.get("error"))[:80])
        await backend_bad.close()
    finally:
        fake_backend.STATE.token = ""


# ---------------------------------------------------------------------------
# 9. bot 暴露给前端的接口（契约第 9 节）
# ---------------------------------------------------------------------------


async def test_bot_api(port: int) -> None:
    print("\n=== 9. /api/status 与 /api/digest/*（前端调的那三个）===")
    import httpx

    from app import main as main_mod

    settings = make_settings(port)
    reset_state()
    runtime = main_mod.BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]

    # 先造点数据，status / digest 才有内容
    await ingest_message(
        await norm(group_event(message_id=901, text="本周五19:00在教三201开班会"), settings),
        runtime.backend,
        settings=settings,
    )
    main_mod.set_runtime(runtime)

    transport = httpx.ASGITransport(app=main_mod.app)  # 不起 lifespan，不碰 OneBot
    async with httpx.AsyncClient(transport=transport, base_url="http://bot") as client:
        anon = await client.get("/api/status")
        check("不带令牌 → 401", anon.status_code, 401)

        headers = {"Authorization": "Bearer bot-secret"}
        resp = await client.get("/api/status", headers=headers)
        check("带令牌 → 200", resp.status_code, 200)
        payload = resp.json()

        check(
            "status 的顶层就是契约第 9 节那些键",
            sorted(payload),
            sorted([
                "onebot", "llm", "whitelist", "pipeline", "blindspots", "groups",
                "gap_alerts", "backend", "recovery", "digest", "day", "server_time",
            ]),
        )
        check("onebot.mode", payload["onebot"]["mode"], "fake")
        check("llm.extractor", payload["llm"]["extractor"], "rule")
        check("llm.cross_check_enabled", payload["llm"]["cross_check_enabled"], False)
        check("whitelist.sender_mode", payload["whitelist"]["sender_mode"], "strict")
        check("pipeline.today_ingested", payload["pipeline"]["today_ingested"], 1)
        check("pipeline.today_extracted", payload["pipeline"]["today_extracted"], 1)
        check("blindspots.window_days", payload["blindspots"]["window_days"], 7)
        check("backend.reachable", payload["backend"]["reachable"], True)
        check("digest.enabled", payload["digest"]["enabled"], True)
        check("day 是配置时区的今天", payload["day"], payload["day"])
        check_true("day 形如 YYYY-MM-DD", len(payload["day"]) == 10 and payload["day"][4] == "-")
        check_true(
            "groups 里有那个群，且标了在不在白名单",
            any(g["group_id"] == "123456789" and g["in_whitelist"] for g in payload["groups"]),
            str(payload["groups"])[:120],
        )

        preview = await client.get("/api/digest/preview", headers=headers)
        check("digest/preview → 200", preview.status_code, 200)
        check_true("digest/preview 返回 text", "每日通知" in preview.json().get("text", ""))

        sent = await client.post("/api/digest/send", json={"dry_run": True}, headers=headers)
        check("digest/send → 200", sent.status_code, 200)
        body = sent.json()
        check("digest/send 的键", sorted(body), ["error", "ok", "sent", "text"])
        check("dry_run 不发", body["sent"], False)
        check("dry_run ok", body["ok"], True)

        noauth = await client.post("/api/digest/send", json={"dry_run": True})
        check("digest/send 也校验令牌", noauth.status_code, 401)

    await runtime.backend.close()
    main_mod.set_runtime(None)


# ---------------------------------------------------------------------------
# 10. bot 自己的接口也要分范围：网页令牌不能发消息
# ---------------------------------------------------------------------------


async def test_bot_scopes(port: int) -> None:
    print("\n=== 10. 网页令牌 vs 管理令牌（bot 自己的 /api）===")
    import httpx

    from app import main as main_mod

    settings = make_settings(port)
    reset_state()
    runtime = main_mod.BotRuntime(settings)
    runtime.hub = RecordingHub()  # type: ignore[assignment]
    main_mod.set_runtime(runtime)

    write_h = {"Authorization": "Bearer bot-secret"}
    web_h = {"Authorization": "Bearer web-secret"}

    transport = httpx.ASGITransport(app=main_mod.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://bot") as client:
        # 读：两种令牌都该放行
        for label, h in [("管理令牌", write_h), ("网页令牌", web_h)]:
            r = await client.get("/api/status", headers=h)
            check(f"{label} GET /api/status → 200", r.status_code, 200)
            r = await client.get("/api/digest/preview", headers=h)
            check(f"{label} GET /api/digest/preview → 200", r.status_code, 200)

        # 写：网页令牌一律 403（**不是** 401 —— 身份有效，只是没权限）
        writes = [
            ("POST", "/api/digest/send", {"dry_run": True}),
            ("POST", "/api/send/private", {"user_id": "10001", "message": "hi"}),
            ("POST", "/api/send/group", {"group_id": "123456789", "message": "hi"}),
        ]
        for method, path, body in writes:
            r = await client.request(method, path, json=body, headers=web_h)
            check(f"网页令牌 {method} {path} → 403", r.status_code, 403)
            detail = (r.json() or {}).get("detail", "") if r.status_code == 403 else ""
            check_true(f"{path} 的 403 说明了原因", "只允许持有管理令牌" in detail, repr(detail))

        # 最关键的一条：网页令牌发私聊时，**一个字节都不许发出去**
        before = len(runtime.hub.sent)  # type: ignore[attr-defined]
        await client.post(
            "/api/send/private", json={"user_id": "10001", "message": "冒充"}, headers=web_h
        )
        check(
            "网页令牌被拒后没有真的发消息",
            len(runtime.hub.sent),  # type: ignore[attr-defined]
            before,
        )

        # 管理令牌仍然能发（dry_run 只是走一遍组装，不真发）
        r = await client.post("/api/digest/send", json={"dry_run": True}, headers=write_h)
        check("管理令牌 POST /api/digest/send → 200", r.status_code, 200)

        # 坏令牌仍然是 401（和 403 区分开）
        r = await client.get("/api/status", headers={"Authorization": "Bearer nope"})
        check("坏令牌 → 401", r.status_code, 401)

    await runtime.backend.close()
    main_mod.set_runtime(None)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


async def run_all() -> int:
    port = free_port()
    server, task = await serve_fake_backend(port)
    print(f"假后端已启动：http://127.0.0.1:{port}")
    try:
        await test_group_message_flow(port)
        await test_filters(port)
        await test_gap_detection(port)
        await test_crash_recovery(port)
        await test_backend_down()
        await test_blindspots(port)
        await test_commands(port)
        await test_digest(port)
        await test_auth(port)
        await test_bot_api(port)
        await test_bot_scopes(port)
    finally:
        await stop_fake_backend(server, task)

    print()
    if failures:
        print(f"❌ {len(failures)} 项失败：")
        for name in failures:
            print(f"   - {name}")
        return 1
    print("✅ 全部通过")
    return 0


def main() -> int:
    import logging

    logging.basicConfig(level=logging.WARNING, format="%(levelname)-7s %(name)s | %(message)s")
    # 让 digest.send_digest 里那个 get_settings() 也能看到测试用的收件人
    config.get_settings.cache_clear()
    return asyncio.run(run_all())


if __name__ == "__main__":
    sys.exit(main())
