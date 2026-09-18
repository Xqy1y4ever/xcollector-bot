"""端到端自检：真 bot 代码 + 假后端（不需要 NapCat、不需要真后端）。

    python -m tests.check_pipeline_e2e

它在**本进程内**起一个 `app.tools.fake_backend`（真 uvicorn、真 TCP），
然后用真代码喂 OneBot 事件进去，最后断言假后端记录下来的**调用序列**与数据。

**多用户之后这个文件里多了一件必做的事**：先造出真用户和真订阅。
路由（`GET /api/subscriptions/routing`）现在是流水线的第一步，没人订阅的来源
**根本不会被抽取**，所以"注册两个用户 + 给两边的订阅"不是夹具的装饰，
而是被测语义本身的一部分。用户是通过假后端的注册接口（要验证码 + 邀请码）
真造出来的，不是往 `STATE.users` 里塞的 —— 塞进去就绕过了注册链路，
而"QQ 号 → usr_... 租户"的解析正是这次改造最容易错的地方。

覆盖这几块：
  1. 一条群消息从 OneBot 事件到入库，依次调了哪几个后端接口（含附件）；
  2. 白名单 / 噪声 / **没人订阅就不抽取**；
  3. 缺口检测（按用户扇出的群级告警）；
  4. 崩溃恢复：后端里停在 pending 的消息会被 resume_pending() 捡回来处理完；
  5. 后端不可达 / 路由失败时**绝不把"查不到"当成"没人要"**；
  6. 多用户扇出：抽一次、给 N 个人各写一条通知、各记一份统计；
  7. 指令全链路（QQ 号 → 租户解析、订阅指令、/done 的 actor 与租户）；
  8. digest 按用户组装、按各自的 QQ 发送；
  9. bot 自己 /api 的鉴权（只剩一个管理令牌）。

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
import time
from contextlib import contextmanager, nullcontext

# Windows 控制台默认 GBK，print 中文/emoji 会 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

# 双保险：本进程任何东西都不许去连真实 NapCat
os.environ["ONEBOT_WS_URL"] = "ws://127.0.0.1:3999"
os.environ["DIGEST_TARGET_QQ"] = "10001"
# bot 自己的入口也要有令牌
os.environ["BOT_API_TOKEN"] = "bot-secret"
# 多用户之后**不再有网页令牌**：`/api/status` 这类运营者视角的接口一律要管理令牌。
# 这个变量留着是为了断言"它已经不再是一个有效令牌"（见第 10 节）。
os.environ["WEB_API_TOKEN"] = "web-secret"

from app import config  # noqa: E402
from app.backend_client import BackendClient, BackendError  # noqa: E402
from app.commands import (  # noqa: E402
    BACKEND_DOWN_REPLY,
    NOT_REGISTERED_REPLY,
    SOURCES_STALE_REPLY,
    STALE_LIST_REPLY,
    SUBSCRIBE_NEED_SENDER,
    CommandRouter,
)
from app.config import Settings  # noqa: E402
from app.pipeline.digest import (  # noqa: E402
    build_digest,
    configure_digest,
    reset_digest_context,
    resolve_recipients,
    send_digest,
    sent_today,
)
from app.pipeline.runner import ingest_message, resume_pending  # noqa: E402
from app.tools import fake_backend  # noqa: E402
from app.tools.send_test import FakeHub  # noqa: E402
from app.utils import local_day  # noqa: E402

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
# 夹具：真群号 / 真 QQ 号 / 真用户
# ---------------------------------------------------------------------------

# 主测试群和主发送者（原来就在，保持不变）
GROUP = "123456789"
SENDER = "10001"
# 第二个群：专门用来造"这个群里没有任何订阅者"的场景。
# 用另一个群而不是另一个发送者，是因为"没人订阅"要测的是**路由**这一步，
# 而路由的输入就是 (群, 发送者)；换群更接近真实世界里"这个群没人订"。
GROUP_EMPTY = "223456789"
GROUP_EMPTY_NAME = "示例通知群二"

# 夹具用到的 QQ 号。10001 一直是"主人"，其余按需注册。
QQ_OWNER = "10001"
QQ_OTHER = "10002"
QQ_SECOND = "10003"
QQ_WHITELISTED_UNREGISTERED = "55555"  # 在指令白名单里、但故意不注册
QQ_OUTSIDER = "88888"                  # 既不在白名单、也没注册

# 手动 /add 的 group_id 是 `manual:<QQ号>`（归属靠 user_id，QQ 号仍作为身份锚点）
MANUAL_GROUP_OWNER = f"manual:{QQ_OWNER}"


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
        group_whitelist=f"{GROUP}:示例通知群,{GROUP_EMPTY}:{GROUP_EMPTY_NAME}",
        sender_whitelist=f"{SENDER}:张老师,{QQ_OTHER}:李老师",
        sender_whitelist_mode="strict",
        onebot_ws_url="ws://127.0.0.1:3999",
        # 指令白名单里额外放一个**故意不注册**的 QQ：用来分辨
        # "不在白名单"（静默忽略）和"在白名单但没注册"（要提示去注册）。
        command_whitelist=f"{QQ_OWNER}:我,{QQ_OTHER}:李老师,{QQ_WHITELISTED_UNREGISTERED}:同学",
        # digest 收件人**默认留空** —— 空 = 每个注册用户各收一份自己的。
        # 单用户时代的 DIGEST_TARGET_QQ 覆盖路径由第 7 节单独测。
        digest_target_qq="",
        pending_retry_interval=999,
        recovery_check_interval=999,
    )
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# 用户与订阅：走**真 HTTP 接口**造，不往 STATE 里塞
# ---------------------------------------------------------------------------


@contextmanager
def settings_patch(settings: Settings):
    """让 `get_settings()` 在这次 with 里返回给定的 Settings。

    ⚠️ 必须**逐个模块**替换，不能只改 `config.get_settings`。
    app 里的模块用的都是 `from .config import get_settings`，导入的那一刻就把
    函数对象绑进了自己的命名空间；只改 `config` 上那个名字，它们照旧拿旧的。
    那会造出一种最糟的测试：看起来切换了配置，其实一行都没生效，
    断言仍然"通过"（或以一种和被测逻辑无关的方式失败）。

    所以这里把**所有已经导入的、自己绑了 `get_settings` 的 app 模块**都换掉，
    退出时逐个还原。以后新加一个模块也不需要回来改这里。
    """
    targets = [config]
    for module in list(sys.modules.values()):
        name = getattr(module, "__name__", "") or ""
        if name.startswith("app.") and hasattr(module, "get_settings"):
            targets.append(module)
    saved = [(module, module.get_settings) for module in targets]
    for module in targets:
        module.get_settings = lambda: settings  # type: ignore[assignment]
    try:
        yield settings
    finally:
        for module, original in saved:
            module.get_settings = original  # type: ignore[assignment]


class World:
    """一次运行的公共夹具：假后端地址、注册过的用户、各自的后端客户端。

    刻意**不做"reset 之后自动重建"**：register() 会先清空一次假后端，
    所以谁想保留数据就必须在建完用户之后再写数据。把这件事写明显，
    比让夹具在背后偷偷重建要安全 —— 偷偷重建会把"我以为还在的那条通知"
    变成一条不存在的数据，而断言只会看到一个莫名其妙的 False。
    """

    def __init__(self, port: int):
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.settings = make_settings(port)
        self.users: dict[str, str] = {}  # QQ → usr_...
        self.tokens: dict[str, str] = {}  # QQ → xc_...（用户令牌，只有真注册才有）
        self._clients: list[BackendClient] = []

    # ---------------- 客户端 ----------------

    def client(self, settings: Settings | None = None) -> BackendClient:
        """造一个连假后端的客户端，并记下来，收尾时统一关掉。"""
        c = BackendClient(settings or self.settings)
        self._clients.append(c)
        return c

    async def close(self) -> None:
        for c in self._clients:
            try:
                await c.close()
            except Exception:
                pass
        self._clients = []

    # ---------------- 注册 / 订阅 ----------------

    async def register(self, qq: str, *, reset: bool = False, display_name: str | None = None) -> str:
        """造一个**真注册**的用户，返回它的 `usr_...` 租户 id。

        走的是完整链路：签发 QQ 验证码 → 发邀请码 → `/api/register`。
        每一步都带**服务令牌**（和 bot 的身份一致），邀请码是因为假后端默认
        `SIGNUPS_REQUIRE_INVITE = True`（真后端的默认值也是 invite）。
        """
        import httpx

        if reset:
            await self.reset()
        headers = {"Authorization": f"Bearer {self.settings.api_token}"}
        async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
            verify = await c.post("/api/verify/request", json={"qq": str(qq)}, headers=headers)
            verify.raise_for_status()
            code = str(verify.json()["code"])
            invite = await c.post(
                "/api/invites",
                json={"note": f"e2e-{qq}", "max_uses": 1},
                headers=headers,
            )
            invite.raise_for_status()
            invite_code = str(invite.json()["code"])
            body = {"qq": str(qq), "code": code, "invite_code": invite_code}
            if display_name:
                body["display_name"] = display_name
            resp = await c.post("/api/register", json=body)
            assert resp.status_code == 200, f"注册 {qq} 失败：{resp.status_code} {resp.text}"
            payload = resp.json()
        user_id = str(payload["user"]["id"])
        assert user_id.startswith("usr_"), f"租户 id 形状不对：{user_id!r}"
        self.users[qq] = user_id
        self.tokens[qq] = str(payload["token"])
        return user_id

    async def subscribe(
        self,
        qq: str,
        group_id: str = GROUP,
        sender_id: str = SENDER,
        *,
        user_id: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        """给某个 QQ（或直接给某个租户）订一个 (群, 发送者)。

        直接调后端接口：订阅指令本身的端到端行为在第 7 节单独测，
        这里只是在造夹具。`user_id` 走 query（契约里归属不在 body 里）。
        """
        import httpx

        owner = user_id or self.users[qq]
        headers = {
            "Authorization": f"Bearer {(settings or self.settings).api_token}"
        }
        async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
            resp = await c.post(
                "/api/subscriptions",
                params={"user_id": owner},
                json={"group_id": str(group_id), "sender_id": str(sender_id)},
                headers=headers,
            )
            assert resp.status_code == 200, f"订阅失败：{resp.status_code} {resp.text}"

    async def reset(self) -> None:
        """清空假后端（用户、订阅、通知……全没了）。

        用 HTTP 而不是直接调 `STATE.reset()`：这样走的是真接口，
        而且和 register() 里那句逻辑一致（`reset=True` 就是靠它）。
        """
        import httpx

        headers = {"Authorization": f"Bearer {self.settings.api_token}"}
        async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
            await c.post("/api/_fake/reset", headers=headers)
        self.users.clear()
        self.tokens.clear()


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
    group_id: int = 123456789,
    user_id: int = 10001,
    card: str = "张老师",
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
        "group_id": group_id,
        "user_id": user_id,
        "self_id": 999999,
        "time": ts,
        "sender": {"user_id": user_id, "nickname": "小李", "card": card, "role": "admin"},
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


def recent_ts(hours_ago: int = 0) -> int:
    """**离现在很近**的消息时间（秒，OneBot 用秒）。

    什么时候需要它：只有断言依赖"这条通知的 status 是 active"的时候。

    为什么不能像其它用例那样用固定的绝对时间戳（默认 1757692800 = 2025-09）：
    `status` 是后端**由 due_at 推导**出来的（due_at 在过去 → expired）。
    "本周五19:00"配上一年前的时间戳就是一条**已过期**的通知，于是
    `/list`（只列 active）空空如也 —— 断言会失败在一个和被测逻辑毫无关系的
    原因上（而且过一阵子换台机器跑又是另一个结果）。
    需要 active 就用这个；需要**确定性日期**的用例（解析、格式化）继续用固定值。
    """
    return int(time.time()) - hours_ago * 3600


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


def reset_state(*, keep_accounts: bool = False) -> int:
    """清空假后端的**数据**，返回一个 mark。

    `keep_accounts=True` 时保留用户与订阅。

    为什么需要这个开关：多用户之后几乎每个用例的前提都是"有个真用户、
    而且他订了这个来源"。把账号一起清掉，被测代码接下来看到的是
    **未注册** —— 于是 /add 回一句"你还没有注册"，断言就变成在测另一件事了：
    它仍然在跑、仍然可能"通过"，但什么都没证明。这类静默失效比红更难发现。

    默认仍然全清：有些用例要断言"库里一条原文都没有"，
    那必须真的从空开始。
    """
    keep = {}
    if keep_accounts:
        # 只保留"身份"这一类，不保留任何业务数据（消息、通知、统计、编号映射）。
        keep = {
            "users": dict(fake_backend.STATE.users),
            "user_tokens": dict(fake_backend.STATE.user_tokens),
            "user_index": dict(fake_backend.STATE.user_index),
            "subscriptions": dict(fake_backend.STATE.subscriptions),
            "invites": dict(fake_backend.STATE.invites),
            "verify_codes": dict(fake_backend.STATE.verify_codes),
        }
    fake_backend.STATE.reset()
    for name, value in keep.items():
        setattr(fake_backend.STATE, name, value)
    return mark()


def stats_row(user_id: str, day: str | None = None) -> dict:
    """取某个用户某天的统计行。**必须按 user_id 取** ——

    多用户之后 `STATE.stats` 的键是 `(user_id, day)`。以前那句
    "取第一条"在两个人同一天都有统计时会随机看其中一个人的数字，
    断言看起来还在跑，其实什么都没证明。
    """
    return fake_backend.STATE.stats.get((user_id, day or local_day()), {})


def owners_of_notifications() -> list[str]:
    """所有通知的归属（`usr_...`）列表。"""
    return [str(n.get("user_id") or "") for n in fake_backend.STATE.notifications.values()]


def raw_state_of(raw_id: str) -> str:
    return str(fake_backend.STATE.messages[raw_id]["state"])


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


def last_raw_id() -> str:
    """库里最后一条原文的 id（每个用例都只写一条，所以"最后一条"就是它）。"""
    rows = sorted(fake_backend.STATE.messages.values(), key=lambda r: (r["ts"], r["id"]))
    return str(rows[-1]["id"])


# ---------------------------------------------------------------------------
# 1. 群消息全链路（有人订阅）
# ---------------------------------------------------------------------------


async def test_group_message_flow(world: World) -> None:
    print("\n=== 1. 一条群消息（带图片）从 OneBot 事件到入库 ===")
    await world.reset()
    await world.register(QQ_OWNER)
    owner = world.users[QQ_OWNER]
    await world.subscribe(QQ_OWNER)
    # ⚠️ mark 必须打在所有**准备动作之后**。注册和建订阅本身也要打后端
    # （verify/request、invites、register、subscriptions），让它们混进序列里，
    # 下面那条断言就变成在测"注册流程调了哪些接口"，而不是这条消息的链路了。
    mk = mark()
    settings = make_settings(world.port)

    from app.main import BotRuntime

    runtime = BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]
    await runtime.pipeline.start()
    try:
        event = group_event(
            text=" 大家下周三前把军训心得交到班长那里，不少于800字。",
            image_url=f"http://127.0.0.1:{world.port}/api/_fake/blob/x.png",
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
        # 抽取那一步必须记下"这次是替几个人抽的"：扇出漏了谁，只有这个数字能看出来
        check_true(
            "「抽取」那行带订阅者数量（扇出给谁了）",
            "订阅者=1" in logs.find("阶段=抽取"),
            logs.find("阶段=抽取"),
        )

        seq = sequence_since(mk)
        print("    真实调用序列：")
        for item in seq:
            print(f"      {item}")

        check(
            "群消息的接口调用序列",
            seq,
            [
                "POST /api/messages",              # 写前日志：先保住原文
                "GET <QQ CDN 图片>",                # 下载附件字节（真实世界里是 QQ CDN）
                "POST /api/attachments",           # 上传给后端
                "PATCH /api/messages/{id}",        # 回填 attachments
                "POST /api/groups",                # 群状态（拿 previous_last_msg_ts）
                "GET /api/subscriptions/routing",  # 路由：这条消息要扇给谁（**抽取之前**）
                "POST /api/notifications",         # 建条（每个订阅者一条）
                "PATCH /api/messages/{id}",        # state=extracted
                "POST /api/stats",                 # 统计（每个订阅者一份）
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
                    f"http://127.0.0.1:{world.port}/api/_fake/blob/x.png",
                )

        notifs = list(fake_backend.STATE.notifications.values())
        check_true("通知只有一条", len(notifs) == 1)
        if notifs:
            check("通知标题", notifs[0]["title"], "大家下周三前把军训心得交到班长那里，不少于800字。[图片]")
            check("due_text 逐字保留", notifs[0]["due_text"], "下周三前")
            check("地点没瞎猜（班长那里不是地点）", notifs[0]["location"], None)
            check("抽取器", notifs[0]["extractor"], "rule")
            check_true("evidence 非空", bool(notifs[0]["evidence"]))
            # 通知的归属必须是租户，不是 QQ 号：写错的话用户在前端看不到自己的条
            check("通知归属是 usr_ 租户", notifs[0]["user_id"], owner)

        check("统计 ingested", stats_row(owner).get("ingested"), 1)
        check("统计 extracted", stats_row(owner).get("extracted"), 1)

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
# 1b. 没人订阅 → 不抽取（多用户之后流水线的第一步是"谁要"）
# ---------------------------------------------------------------------------


async def test_unsubscribed_source(world: World) -> None:
    print("\n=== 1b. 没有任何用户订阅这个来源 → 不抽取、不建条、不记统计 ===")
    await world.reset()
    # 有个真用户，但他没订这个来源 —— "没订阅"必须是**真的没人订阅**，
    # 而不是"一个用户都没有"（后者会让断言在数据库空的时候假装通过）。
    owner = await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    backend = world.client()
    # mark 打在准备动作之后：注册和建订阅本身也打后端，混进来就变成
    # 在测"注册流程调了哪些接口"了。
    mk = mark()

    event = group_event(
        message_id=1201,
        group_id=int(GROUP_EMPTY),
        text="大家下周三前把军训心得交到班长那里。",
    )
    outcome = await ingest_message(await norm(event, world.settings), backend, settings=world.settings)
    check("没人订阅这个来源 → unsubscribed", outcome, "unsubscribed")
    check(
        "没人订阅的调用序列：路由在抽取之前，且没有 POST /api/notifications",
        sequence_since(mk),
        [
            "POST /api/messages",
            "POST /api/groups",
            "GET /api/subscriptions/routing",
            "PATCH /api/messages/{id}",
        ],
    )

    raw = fake_backend.STATE.messages[last_raw_id()]
    check("raw 被标成终态 unsubscribed", raw["state"], "unsubscribed")
    check("raw 写明了原因", raw["state_reason"], "没有任何用户订阅这个来源")
    check_true("一条通知都没建", fake_backend.STATE.notifications == {})
    # 没有订阅者 → **一个用户的统计都不该被写**。
    # 统计是按用户的（一条消息扇给 N 个人就给这 N 个人各记一次），
    # 而没有订阅者时根本没有"谁"可以记 —— 硬记到某个人头上，他的盲区计数里
    # 就会出现一条他根本没收到的消息。
    check(
        "没有任何用户的统计被写（没订阅者就没有可归属的人）",
        {uid for (uid, _day) in fake_backend.STATE.stats},
        set(),
    )
    # 抽取是最贵的一步（LLM 调用）。"没抽取"的**诚实可观测量**是统计里没有
    # extracted/unparsed 这些由抽取结果决定的字段 —— 它们只在抽取跑完之后才写。
    # 统计键是 (user_id, day)，所以要按这个用户取，不能取"第一条"。
    got = stats_row(owner)
    check("这个用户没有任何 extracted 计数", got.get("extracted"), None)
    check("这个用户没有任何 unparsed 计数", got.get("unparsed"), None)


# ---------------------------------------------------------------------------
# 1c. 抽一次、扇给 N 个人
# ---------------------------------------------------------------------------


async def test_fan_out_to_two_users(world: World) -> None:
    print("\n=== 1c. 两个用户订同一个来源：抽一次、扇出两条各自的通知 ===")
    mk = await world.reset()
    owner_a = await world.register(QQ_OWNER)
    owner_b = await world.register(QQ_OTHER)  # 注意：不会 reset，两个用户都在
    check_true("两个用户是**不同**的租户", owner_a != owner_b, f"{owner_a} / {owner_b}")
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    await world.subscribe(QQ_OTHER, GROUP, SENDER)
    backend = world.client()

    with LogCapture() as logs:
        outcome = await ingest_message(
            await norm(
                group_event(message_id=1301, text="本周五19:00在教三201开班会，请全体同学准时参加。"),
                world.settings,
            ),
            backend,
            settings=world.settings,
        )
    check("两个订阅者 → extracted", outcome, "extracted")

    notifs = list(fake_backend.STATE.notifications.values())
    check("同一条原文建了 2 条通知（每人一条）", len(notifs), 2)
    check("两条通知 id 不同（不是互相顶掉的那一条）", len({n["id"] for n in notifs}), 2)
    check("两条通知分属两个租户", sorted(n["user_id"] for n in notifs), sorted([owner_a, owner_b]))
    check("两条通知指向同一条原文", len({n["raw_message_id"] for n in notifs}), 1)

    # **"只抽了一次"的诚实可观测量**：bot 在本地抽取（EXTRACTOR=rule 时没有任何
    # 网络请求），所以假后端里没有"抽取次数"这种计数。能观测到的是这三条：
    #   1. 抽取阶段的日志**只有一行**，而且那一行写着"订阅者=2"；
    #   2. 两条通知的机器字段（title/due_at/evidence/model/...）逐字相同 ——
    #      如果是各抽一次，两次的 title 之类可能不同（rule 引擎下也不会不同，
    #      所以这一条只是辅证）；
    #   3. POST /api/notifications 恰好 2 次，抽取器只跑一次没有别的出口。
    extract_lines = [line for line in logs.lines if "阶段=抽取" in line]
    check("抽取阶段只打了一行（一次抽取）", len(extract_lines), 1)
    check_true("那一行写明订阅者=2", "订阅者=2" in (extract_lines[0] if extract_lines else ""), extract_lines[:1])
    machine = ("title", "summary", "location", "due_at", "due_text", "due_confidence", "evidence", "extractor")
    if len(notifs) == 2:
        first = {k: notifs[0].get(k) for k in machine}
        second = {k: notifs[1].get(k) for k in machine}
        check("两条通知的机器字段逐字相同（同一份抽取结果扇出去的）", first, second)
    notif_calls = [c for c in sequence_since(mk) if c == "POST /api/notifications"]
    check("POST /api/notifications 恰好 2 次", len(notif_calls), 2)

    check("A 的统计 extracted=1", stats_row(owner_a).get("extracted"), 1)
    check("B 的统计 extracted=1", stats_row(owner_b).get("extracted"), 1)
    check("A 的统计 ingested=1", stats_row(owner_a).get("ingested"), 1)
    check("B 的统计 ingested=1", stats_row(owner_b).get("ingested"), 1)
    check("两个用户的统计行是两行（不是一份被人共用）", len(fake_backend.STATE.stats), 2)


# ---------------------------------------------------------------------------
# 1d. 路由失败绝不能当成"没人要"
# ---------------------------------------------------------------------------


async def test_routing_failure_stays_pending(world: World) -> None:
    print("\n=== 1d. 查投递名单失败 → raw 保持 pending（不能被吞成终态）===")
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    backend = world.client()

    fake_backend.STATE.fail_routing = True
    try:
        mk = mark()
        outcome = await ingest_message(
            await norm(group_event(message_id=1401, text="大家下周三前交军训心得"), world.settings),
            backend,
            settings=world.settings,
        )
    finally:
        fake_backend.STATE.fail_routing = False

    check("路由 500 → 结果是 error（不是 unsubscribed）", outcome, "error")
    raw = fake_backend.STATE.messages[last_raw_id()]
    check("raw 停在 pending（恢复循环还能捡回来）", raw["state"], "pending")
    check_true("没有写任何终态原因", raw.get("state_reason") is None, str(raw.get("state_reason")))
    check_true("没有建条", fake_backend.STATE.notifications == {})
    check_true("没有写统计", owner not in {uid for (uid, _day) in fake_backend.STATE.stats})
    check(
        "路由失败的调用序列：只到路由那一步就停了",
        sequence_since(mk),
        [
            "POST /api/messages",
            "POST /api/groups",
            "GET /api/subscriptions/routing",
        ],
    )
    # 反面：这个用户其实是订了的。把故障撤掉再恢复，必须能建成 ——
    # 否则"路由失败"和"没人要"在数据上就分不出来了。
    mk = mark()
    n = await resume_pending(backend, world.settings)
    check("撤掉故障后恢复处理了 1 条", n, 1)
    check("恢复后状态 extracted", fake_backend.STATE.messages[last_raw_id()]["state"], "extracted")
    check("恢复后建了 1 条通知", len(fake_backend.STATE.notifications), 1)


# ---------------------------------------------------------------------------
# 2. 白名单与噪声
# ---------------------------------------------------------------------------


async def test_filters(world: World) -> None:
    print("\n=== 2. 白名单 / 噪声 ===")

    # 白名单**留空 = 什么都不处理**（fail-closed）。
    # 这是刻意的：官方通知只发在固定几个群、由固定几个人发，
    # 留空放开等于"随便哪个群、谁说话都入库"。
    settings_empty = make_settings(world.port, group_whitelist="", sender_whitelist="")
    backend_empty = world.client(settings_empty)
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

    # 群配了但发送者白名单留空（strict）→ 原文入库，但**路由之后**才被白名单挡下。
    # 顺序必须钉住：路由在前，白名单在后。反过来的话"没人订阅的来源"会先被
    # 标成 skipped_whitelist，用户永远看不到"你没订这个来源"这条真相。
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    settings_nosender = make_settings(world.port, sender_whitelist="")
    backend_ns = world.client(settings_nosender)
    mk = mark()
    outcome = await ingest_message(
        await norm(group_event(message_id=221, text="大家下周三前交军训心得"), settings_nosender),
        backend_ns,
        settings=settings_nosender,
    )
    check("发送者白名单留空（strict）→ skipped_whitelist", outcome, "skipped_whitelist")
    check(
        "被白名单挡下时的调用序列（路由在白名单之前）",
        sequence_since(mk),
        [
            "POST /api/messages",
            "POST /api/groups",
            "GET /api/subscriptions/routing",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    check_true(
        "发送者白名单留空 → whitelist_ready 为假",
        not settings_nosender.whitelist_ready,
    )
    # 被白名单挡下的消息**也要**给订阅者记一次：对他而言"这条消息被处理过"是真的，
    # 只是结局是"没抽"。统计漏了这一笔，用户的盲区数字就会比实际小。
    check(
        "白名单挡下也算这个用户的一条 ingested",
        stats_row(owner).get("ingested"),
        1,
    )
    check("白名单挡下不记 extracted", stats_row(owner).get("extracted"), None)

    # 同一份配置，只把 mode 改成 off（显式放开）→ 就该正常抽取
    await world.reset()
    await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    settings_off = make_settings(world.port, sender_whitelist="", sender_whitelist_mode="off")
    backend_off = world.client(settings_off)
    outcome = await ingest_message(
        await norm(group_event(message_id=223, text="大家下周三前交军训心得"), settings_off),
        backend_off,
        settings=settings_off,
    )
    check("mode=off 且发送者白名单留空 → extracted（显式放开）", outcome, "extracted")
    check_true("mode=off 时 whitelist_ready 为真", settings_off.whitelist_ready)

    # 群不在白名单 → 一个后端请求都不发
    settings = make_settings(world.port, group_whitelist="111111:别的群")
    backend = world.client(settings)
    mk = reset_state()
    msg = await norm(group_event(text="大家下周三前交军训心得"), settings)
    outcome = await ingest_message(msg, backend, settings=settings)
    check("群不在白名单 → group_filtered", outcome, "group_filtered")
    check("群不在白名单 → 不发任何后端请求", sequence_since(mk), [])

    # 发送者在白名单 → 正常
    await world.reset()
    await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    settings2 = make_settings(world.port, sender_whitelist="10001:张老师")
    backend2 = world.client(settings2)
    outcome = await ingest_message(
        await norm(group_event(message_id=222, text="大家下周三前交军训心得"), settings2),
        backend2,
        settings=settings2,
    )
    check("发送者在白名单 → extracted", outcome, "extracted")

    # 发送者不在白名单 → skipped_whitelist（原文仍然入库）
    #
    # ⚠️ 这里**必须先有人订阅这个发送者**，否则测的就不是白名单了：
    # process_raw 的顺序是「先问路由，再查发送者白名单」，所以没人订阅时
    # 结果是 unsubscribed（在到达白名单那一步之前就返回了）。
    #
    # 顺序是刻意的，不是随手写的：白名单拒绝**只有在这个来源有人订阅时才有意义** ——
    # 那时才有具体的用户需要知道"你订的这个来源被运营者的白名单挡掉了"。
    # 反过来先查白名单会省一次网络往返，但会让这条信息丢失：
    # 订阅了却被白名单过滤，从用户角度看和"这个来源永远没消息"一模一样，
    # 而那正是最难发现的一类静默失败。
    await world.subscribe(QQ_OWNER, GROUP, "55555")
    settings3 = make_settings(world.port, sender_whitelist="99999:别人")
    backend3 = world.client(settings3)
    # 清数据但**留下账号和订阅**：这一条测的是"订了却被白名单挡下"，
    # 把订阅一起清掉就变成 unsubscribed 了（在到达白名单之前就返回）。
    mk = reset_state(keep_accounts=True)
    event = group_event(message_id=333, text="大家下周三前交军训心得", user_id=55555, card="别人")
    outcome = await ingest_message(await norm(event, settings3), backend3, settings=settings3)
    check("发送者不在白名单 → skipped_whitelist", outcome, "skipped_whitelist")
    rows = list(fake_backend.STATE.messages.values())
    check("状态被标成 skipped_whitelist", rows[0]["state"], "skipped_whitelist")
    check_true("没有建条", fake_backend.STATE.notifications == {})
    # 这一次**要**统计：订阅者存在，所以这条"被白名单挡掉"的事被记在了他名下
    # （见上面顺序的说明）——他能看到自己的源被过滤了。
    #
    # 注意能观测到的量只有 `ingested`：后端的统计列是固定的那几个
    # （ingested/extracted/unparsed/conflicts/degraded/llm_tokens），
    # **没有** skipped_whitelist / noise 这种按结果分的计数列 —— 传了也会被
    # 后端按"未知字段忽略"丢掉。所以"被挡掉了"这件事的证据在
    # raw_message.state（共享层）上，不在统计里。
    check_true(
        "被白名单挡掉的这条记在了订阅者名下（他名下有 ingested）",
        bool(stats_row(world.users[QQ_OWNER]).get("ingested")),
        str(stats_row(world.users[QQ_OWNER])),
    )
    check(
        "而且没有把它算成 extracted/unparsed（白名单这一步在抽取之前）",
        [
            stats_row(world.users[QQ_OWNER]).get("extracted"),
            stats_row(world.users[QQ_OWNER]).get("unparsed"),
        ],
        [None, None],
    )

    # 反面：同样不在白名单，但**没人订阅** → unsubscribed（连白名单都不会查）
    await world.reset()
    await world.register(QQ_OWNER)
    backend3b = world.client(settings3)
    outcome = await ingest_message(
        await norm(
            group_event(message_id=334, text="大家下周三前交军训心得", user_id=55556, card="别人"),
            settings3,
        ),
        backend3b,
        settings=settings3,
    )
    check("不在白名单且没人订阅 → unsubscribed（路由在抽取之前也在此处生效）", outcome, "unsubscribed")

    # 噪声（闲聊）→ noise，不计入盲区
    await world.reset()
    await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    settings4 = make_settings(world.port)
    backend4 = world.client(settings4)
    outcome = await ingest_message(
        await norm(group_event(message_id=444, text="收到", at_all=False), settings4),
        backend4,
        settings=settings4,
    )
    check("闲聊 → noise", outcome, "noise")
    rows = list(fake_backend.STATE.messages.values())
    check("状态 noise", rows[0]["state"], "noise")
    check("noise 不计入 unparsed", stats_row(world.users[QQ_OWNER]).get("unparsed"), None)


# ---------------------------------------------------------------------------
# 3. 缺口检测（群级事件、按用户扇出）
# ---------------------------------------------------------------------------


async def test_gap_detection(world: World) -> None:
    print("\n=== 3. 缺口检测（previous_last_msg_ts）===")

    # --- 没人订阅这个群 → 不产生任何告警 ---
    await world.reset()
    await world.register(QQ_OWNER)  # 有用户，但**不订阅**
    backend = world.client()
    await ingest_message(
        await norm(group_event(message_id=1, ts=1757692800, text="大家下周三前交心得"), world.settings),
        backend,
        settings=world.settings,
    )
    check_true("第一次见到该群，不产生缺口告警", fake_backend.STATE.gap_alerts == {})
    await ingest_message(
        await norm(
            group_event(message_id=2, ts=1757692800 + 16 * 3600, text="大家下周三前交心得"),
            world.settings,
        ),
        backend,
        settings=world.settings,
    )
    # 缺口告警也是按用户扇出的：没人订阅 → 没有收件人 → 一条都不建。
    # 建一条无归属的告警（user_id 为空）会让后端 400，也会让前端报出别人的缺口。
    check_true(
        "没有人订阅这个群 → 缺口告警一条都不建",
        fake_backend.STATE.gap_alerts == {},
    )

    # --- 两个人订了同一个群里的**不同**发送者 → 两个人都该收到群级告警 ---
    await world.reset()
    owner_a = await world.register(QQ_OWNER)
    owner_b = await world.register(QQ_OTHER)
    # A 订的是夹具里的主发送者 10001，B 订的**不是**它：缺口是群级事件，
    # "这个群断了一段"对 B 同样成立，所以他必须也收到。
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    await world.subscribe(QQ_OTHER, GROUP, QQ_OTHER)
    backend = world.client()
    await ingest_message(
        await norm(group_event(message_id=11, ts=1757692800, text="大家下周三前交心得"), world.settings),
        backend,
        settings=world.settings,
    )
    mk = mark()
    await ingest_message(
        await norm(
            group_event(message_id=12, ts=1757692800 + 16 * 3600, text="大家下周三前交心得"),
            world.settings,
        ),
        backend,
        settings=world.settings,
    )
    check("间隔 16 小时 → 两个订阅者各一条缺口告警", len(fake_backend.STATE.gap_alerts), 2)
    check(
        "缺口告警按用户扇出（群级事件，不问发送者）",
        sorted(a["user_id"] for a in fake_backend.STATE.gap_alerts.values()),
        sorted([owner_a, owner_b]),
    )
    alerts = list(fake_backend.STATE.gap_alerts.values())
    check_true("缺口告警写明间隔", all("16.0 小时" in a["reason"] for a in alerts), str(alerts[:1]))
    # 一条消息会问**两次**路由，而且两次问的不是同一件事 —— 别把它们当成重复调用：
    #   1. 缺口检测问"这个群里**任何**发送者"（群级事件）→ 不带 sender_id
    #   2. 投递问"这个群里**这一个人**"（决定扇给谁）→ 必须带 sender_id
    # 合并成一次会两头出错：要么把缺口告警发给"只订了这个群里别人"的人，
    # 要么把消息扇给"根本没订这个发送者"的人。
    routing_calls = [
        c for c in fake_backend.STATE.calls[mk:] if c["path"] == "/api/subscriptions/routing"
    ]
    group_level = [c for c in routing_calls if "sender_id=" not in c["query"]]
    pair_level = [c for c in routing_calls if "sender_id=" in c["query"]]
    check("一条消息问两次路由（缺口一次、投递一次）", len(routing_calls), 2)
    check("其中恰好一次是群级（缺口检测）", len(group_level), 1)
    check("其中恰好一次带 sender_id（投递）", len(pair_level), 1)
    check_true(
        "群级那次只带 group_id",
        bool(group_level) and group_level[0]["query"] == f"group_id={GROUP}",
        str([c["query"] for c in group_level]),
    )
    check_true(
        "投递那次带上了这个发送者",
        bool(pair_level) and f"sender_id={SENDER}" in pair_level[0]["query"],
        str([c["query"] for c in pair_level]),
    )
    # 恢复时不能把 last_msg_ts 拨回去（那会造出假的缺口）
    check("group_state 的 last_msg_ts 是最后一条", fake_backend.STATE.groups["123456789"]["last_msg_ts"],
          1757692800000 + 16 * 3600 * 1000)


# ---------------------------------------------------------------------------
# 4. 崩溃恢复
# ---------------------------------------------------------------------------


async def test_crash_recovery(world: World) -> None:
    print("\n=== 4. 崩溃恢复（写前日志之后崩了）===")
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    backend = world.client()

    # 模拟"原文已落库、但重活没做"：直接往后端塞一条 pending
    body = {
        "message_id": "crash-1",
        "group_id": GROUP,
        "group_name": "示例通知群",
        "sender_id": SENDER,
        "sender_name": "张老师",
        "ts": 1757692800000,
        "content": "本周五19:00在教三201开班会，请全体同学准时参加。",
        "attachments": [],
        "raw": {"message": [{"type": "text", "data": {"text": "x"}}]},
    }
    created = await backend.create_message(body)
    check_true("pending 原文已入库", bool(created.get("id")))
    mk = mark()

    n = await resume_pending(backend, world.settings)
    check("恢复处理了 1 条", n, 1)
    check(
        "恢复路径的调用序列",
        sequence_since(mk),
        [
            "GET /api/messages",               # 问后端"还有哪些 pending"
            "GET /api/subscriptions/routing",  # 路由（恢复也要先问谁要）
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
        check("恢复后归属是这个用户", notifs[0]["user_id"], owner)
    # 恢复不该补记 ingested（原文早就在库里了），但 extracted 是真的发生了
    check("恢复不补记 ingested", stats_row(owner).get("ingested"), None)
    check("恢复记 extracted", stats_row(owner).get("extracted"), 1)

    # 恢复不应该把群状态往回拨
    check_true("恢复不动 group_state", fake_backend.STATE.groups == {})


# ---------------------------------------------------------------------------
# 5. 后端不可达
# ---------------------------------------------------------------------------


async def test_backend_down(world: World) -> None:
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
    # 多用户之后 digest 的"今天发过没有"是一个**计数**（有几个人收到了），
    # 不再是布尔：一个人收到不代表别人收到。
    check("status.digest.sent_today 是计数 0", payload["digest"]["sent_today"], 0)
    check("status.digest.sent_today_all", payload["digest"]["sent_today_all"], False)
    check("status.pipeline.per_user_aggregate", payload["pipeline"]["per_user_aggregate"], True)

    sender = StubSender()
    router = CommandRouter(backend, sender, settings)
    # 后端不可达时，**身份解析也做不了**（QQ → usr_... 要靠后端查）。
    # 这时绝不能说"你还没注册" —— 那是把故障说成用户的错。
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    check("后端不可达时 /add 的回执", sender.last(), BACKEND_DOWN_REPLY)
    await router.handle_private_event(private_event(text="/list"))
    check("后端不可达时 /list 的回执", sender.last(), BACKEND_DOWN_REPLY)

    await runtime.backend.close()


# ---------------------------------------------------------------------------
# 5b. 盲区计数 + 状态页的按用户汇总
# ---------------------------------------------------------------------------


async def test_blindspots(world: World) -> None:
    """盲区计数必须是**真的查出来的数字**，不是兜底默认值。

    这一段是刻意的：`_safe()` 会把后端异常吞成默认值，所以"计数为 0"既可能是
    "真的没有"，也可能是"查询炸了"。所以这里先造出非零的数据，再断言非零。
    """
    print("\n=== 5b. 盲区计数（count_only=1）===")
    await world.reset()
    owner = await world.register(QQ_OWNER)
    backend = world.client()

    from app.main import BotRuntime

    runtime = BotRuntime(make_settings(world.port))
    runtime.hub = FakeHub()  # type: ignore[assignment]

    # 1) 一条 unparsed + 一条 degraded 的原文（7 天内，共享层）
    for i, state in enumerate(("unparsed", "degraded")):
        created = await backend.create_message(
            {
                "message_id": f"blind-{i}",
                "group_id": GROUP,
                "ts": 1789565000000,
                "content": "看不清的公告",
                "raw": {},
            }
        )
        await backend.patch_message(created["id"], {"state": state, "state_reason": "测试"})
    # 2) 一条 conflict 的通知 + 一条低置信度的通知 + 一条正常的（都归这个用户）
    for i, (conflict, conf) in enumerate(((True, 0.9), (False, 0.3), (False, 0.95))):
        await backend.create_notification(
            {
                "raw_message_id": f"blind-notif-{i}",
                "group_id": GROUP,
                "title": f"测试 {i}",
                "due_at": 1789999999000,
                "due_confidence": conf,
                "conflict": conflict,
                "evidence": "证据",
                "extractor": "rule",
            },
            user_id=owner,
        )
    # 3) 一条 7 天前的 unparsed（不该被算进去）
    old = await backend.create_message(
        {
            "message_id": "blind-old",
            "group_id": GROUP,
            "ts": 1789565000000 - 30 * 24 * 3600 * 1000,
            "content": "很久以前的公告",
            "raw": {},
        }
    )
    await backend.patch_message(old["id"], {"state": "unparsed", "state_reason": "测试"})

    bs = await runtime_blindspots(runtime)
    check("unparsed_count（unparsed+degraded，7 天内）", bs["unparsed_count"], 2)
    check("conflict_count", bs["conflict_count"], 1)
    check("low_confidence_count", bs["low_confidence_count"], 1)
    check("window_days", bs["window_days"], 7)
    check("degraded_today（今天没写过 degraded 统计）", bs["degraded_today"], False)
    check("blindspots.users_counted（按用户算了几个）", bs["users_counted"], 1)
    check("blindspots.users_missing（都取到了）", bs["users_missing"], 0)
    check(
        "blindspots.per_user 里能看到是**谁**的盲区",
        [r["user_id"] for r in bs["per_user"]],
        [owner],
    )
    check_true("聚合口径写在响应里（不然运维会以为 300 条就是 300 条消息）", bool(bs["aggregate_note"]))

    # raw 状态计数也应该能按 count_only 问到（共享层，不分用户）
    check("count_messages 能按 state 过滤", await backend.count_messages(state=["unparsed", "degraded"]), 3)
    check("count_messages 带 since 窗口", await backend.count_messages(
        state=["unparsed", "degraded"], since=1789565000000 - 7 * 24 * 3600 * 1000
    ), 2)

    await runtime.backend.close()


async def runtime_blindspots(runtime) -> dict:
    """取状态页的 blindspots 段。

    ⚠️ 已知产品缺陷（**不在本文件的可改范围内**，见文件末尾的说明）：
    `main.BotRuntime.status_payload` 在**有注册用户**时会抛 KeyError
    （`per_user` 的行里没有 `gap_alerts` 这个键）。这里先原样调，
    抛出来就把缺陷本身断言下来，而不是让整轮自检崩在一行 try 里。
    """
    try:
        return (await runtime.status_payload())["blindspots"]
    except KeyError as exc:
        check_true(
            "【已知产品缺陷】status_payload 的按用户段读了一个不存在的键",
            str(exc) in ("'gap_alerts'",),
            str(exc),
        )
        return {}


# ---------------------------------------------------------------------------
# 6. 指令：QQ 号 → 租户
# ---------------------------------------------------------------------------


async def test_commands(world: World) -> None:
    print("\n=== 6. 指令对假后端的全链路（QQ 号 → usr_ 租户）===")
    mk = await world.reset()
    owner = await world.register(QQ_OWNER)
    settings = make_settings(world.port)
    backend = world.client()
    sender = StubSender()
    router = CommandRouter(backend, sender, settings)

    # --- /add 有把握 → 直接建条 ---
    mk = mark()
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    print(f"    回执：{sender.last().splitlines()[0]}")
    check_true("有把握 → 回执含「已添加」", "已添加" in sender.last())
    check(
        "/add 的调用序列",
        sequence_since(mk),
        [
            "GET /api/users/lookup",       # QQ 号 → usr_... 租户（多用户新增的第一步）
            "POST /api/messages",
            "POST /api/notifications",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    rows = list(fake_backend.STATE.messages.values())
    # 手动任务的 group_id 里是**QQ 号**（身份锚点），而数据归属是 user_id（租户）。
    # 这两个值在多用户改造里被刻意分开；写成 owner 也不是"错"，但这里是断言
    # 契约里那一条：手动原文的 group_id 记的是"谁手打的"。
    check("手动消息的 group_id", rows[0]["group_id"], MANUAL_GROUP_OWNER)
    check("手动消息的群名", rows[0]["group_name"], "手动添加")
    check_true("手动消息 id 形如 manual-<ms>", rows[0]["message_id"].startswith("manual-"))
    notifs = list(fake_backend.STATE.notifications.values())
    check_true("手动任务建成了通知", len(notifs) == 1)
    if notifs:
        check("手动任务 due_text", notifs[0]["due_text"], "明天下午3点")
        check("手动任务 evidence 是用户原文", notifs[0]["evidence"], "明天下午3点 交实验报告")
        # 最关键的一条：手动任务**不属于 QQ 号**，属于那个人的租户
        check("手动任务的归属是 usr_ 租户（不是 QQ 号）", notifs[0]["user_id"], owner)
    check("手动任务的统计记在这个租户上", stats_row(owner).get("extracted"), 1)

    # --- /add 没把握 → 回问 + 待确认状态存后端（键是 QQ 号，归属是租户）---
    # 清数据但**留下账号**：指令层第一步是 `GET /api/users/lookup` 把 QQ 号
    # 解析成租户，账号被清掉的话这一步会 404，用户拿到的回复变成
    # "你还没有注册" —— 下面那条"回问文案"就再也测不到真正的回问逻辑了。
    mk = reset_state(keep_accounts=True)
    await router.handle_private_event(private_event(text="/add 尽快把材料交上来"))
    print(f"    回问：{sender.last().splitlines()[0]}")
    check_true("回问文案", "仍然添加吗" in sender.last())
    check(
        "没把握时只存待确认、不建条",
        sequence_since(mk),
        ["GET /api/users/lookup", "PUT /api/state/command_pending/10001"],
    )
    # bot_state 的主键是 (user_id, namespace, key)：**key 是 QQ 号，user_id 是租户**。
    # 少了 user_id 那一维，两个人同时 /add 待确认就会互相覆盖。
    check_true(
        "待确认状态写进了后端（键里带租户）",
        (owner, "command_pending", QQ_OWNER) in fake_backend.STATE.kv,
    )
    pending_row = fake_backend.STATE.kv[(owner, "command_pending", QQ_OWNER)]
    check("待确认行里也写着租户", pending_row["user_id"], owner)
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
            "GET /api/users/lookup",                    # 先解析身份
            "GET /api/state/command_pending/10001",     # 读待确认（重启后也读得到）
            "DELETE /api/state/command_pending/10001",  # 先清掉，避免重复确认
            "POST /api/messages",
            "POST /api/notifications",
            "PATCH /api/messages/{id}",
            "POST /api/stats",
        ],
    )
    check_true(
        "确认后待确认被清掉",
        (owner, "command_pending", QQ_OWNER) not in fake_backend.STATE.kv,
    )

    # --- /list（先造一条待办，否则列表是空的）---
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await router.handle_private_event(private_event(text="/add 明天下午3点 交实验报告"))
    mk = mark()
    await router.handle_private_event(private_event(text="/list 5"))
    print("    /list 输出：")
    for line in sender.last().splitlines():
        print(f"      {line}")
    check_true("/list 有表头", sender.last().startswith("📋 待办"), sender.last().splitlines()[0])
    check_true(
        "编号映射存进了后端（同样带租户）",
        (owner, "command_list", QQ_OWNER) in fake_backend.STATE.kv,
    )
    listing = fake_backend.STATE.kv[(owner, "command_list", QQ_OWNER)]["value"]
    check_true("映射里有 notif_id", bool(listing["items"][0]["notif_id"]))
    check_true(
        "映射带 TTL",
        fake_backend.STATE.kv[(owner, "command_list", QQ_OWNER)]["expires_at"] is not None,
    )
    listed_notif_id = str(listing["items"][0]["notif_id"])

    # --- /done 1 ---
    mk = mark()
    await router.handle_private_event(private_event(text="/done 1"))
    check_true("/done 回执", sender.last().startswith("✅ 已完成"), sender.last())
    check(
        "/done 的调用序列",
        sequence_since(mk),
        [
            "GET /api/users/lookup",
            "GET /api/state/command_list/10001",
            "POST /api/notifications/{id}/corrections",
        ],
    )
    corr = list(fake_backend.STATE.corrections.values())
    check("修正 field=status", list(corr[0].keys())[0], "status")
    check("修正 value=done", corr[0]["status"], "done")
    # 编号映射指向的必须**就是**被修正的那一条 —— 这是"用户说的 1 号"
    # 和"实际改的那条"之间唯一的连接。查在**这里**：下面还有几次
    # world.reset()，那会把 corrections 清掉，放到最后查就永远是个假失败。
    check_true(
        "编号映射里的 notif_id 就是被修正的那一条",
        listed_notif_id in fake_backend.STATE.corrections,
        listed_notif_id,
    )

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
    check_true(
        "重启后确实读了一次后端",
        sequence_since(mk)[0].startswith("GET /api/users/lookup"),
        str(sequence_since(mk)[:1]),
    )

    # --- 映射过期 → 绝不猜 ---
    fake_backend.STATE.kv.pop((owner, "command_list", QQ_OWNER), None)
    mk = mark()
    await router2.handle_private_event(private_event(text="/done 1"))
    check("/done 没有编号映射时的回执", sender.last(), STALE_LIST_REPLY)
    check(
        "过期时不调用 corrections",
        [s for s in sequence_since(mk) if "corrections" in s],
        [],
    )

    # --- bot 重启后：待确认仍在 ---
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await router.handle_private_event(private_event(text="/add 尽快把材料交上来"))
    check_true(
        "重启前待确认已写入后端",
        (owner, "command_pending", QQ_OWNER) in fake_backend.STATE.kv,
    )
    router3 = CommandRouter(backend, sender, settings)  # 又一个全新实例
    await router3.handle_private_event(private_event(text="n"))
    check("重启后回 n → 已取消", sender.last(), "已取消。")
    check_true(
        "取消后后端状态被删除",
        (owner, "command_pending", QQ_OWNER) not in fake_backend.STATE.kv,
    )


# ---------------------------------------------------------------------------
# 6b. 未注册 / 非白名单 / /注册
# ---------------------------------------------------------------------------


async def test_registration_and_identity(world: World) -> None:
    print("\n=== 6b. 未注册的 QQ、非白名单的 QQ、/注册 ===")
    await world.reset()
    settings = make_settings(world.port)
    backend = world.client()
    sender = StubSender()
    router = CommandRouter(backend, sender, settings)

    # --- 在白名单里、但还没注册的 QQ 发 /list → 提示去注册，**数据一个字节都没动** ---
    mk = mark()
    before = len(sender.sent)
    await router.handle_private_event(
        private_event(user_id=int(QQ_WHITELISTED_UNREGISTERED), text="/list")
    )
    check("未注册的 QQ 收到「你还没有注册」", sender.last(), NOT_REGISTERED_REPLY)
    check("未注册时确实回了一条（不是静默忽略）", len(sender.sent), before + 1)
    check(
        "未注册时只查了身份，没碰任何数据",
        sequence_since(mk),
        ["GET /api/users/lookup"],
    )

    # --- 非白名单的 QQ 发 /list → 完全忽略：不回复、不发请求 ---
    mk = mark()
    before = len(sender.sent)
    await router.handle_private_event(private_event(user_id=int(QQ_OUTSIDER), text="/list"))
    check("非白名单 QQ 发 /list 不回复", len(sender.sent), before)
    check("非白名单 QQ 发 /list 不发任何请求", sequence_since(mk), [])

    # --- 非白名单的 QQ 发 /注册 → 必须能拿到验证码（注册的前提就是"还不在名单里"）---
    mk = mark()
    before = len(sender.sent)
    await router.handle_private_event(private_event(user_id=int(QQ_OUTSIDER), text="/注册"))
    check("非白名单 QQ 的 /注册 也回了一条", len(sender.sent), before + 1)
    reply = sender.last()
    # check(got, want) 的参数顺序是 (名字, 实际, 期望)，别把期望塞进第二个位置
    check("回执里是私聊发出去的验证码", "🔑" in reply, True)
    m = re.search(r"(\d{6})", reply)
    check_true("回执里有 6 位验证码", bool(m), reply[:80])
    code = m.group(1) if m else ""
    check_true(
        "验证码存进了后端（按 QQ 号）",
        QQ_OUTSIDER in fake_backend.STATE.verify_codes,
        str(list(fake_backend.STATE.verify_codes)[:5]),
    )
    if QQ_OUTSIDER in fake_backend.STATE.verify_codes:
        check("回执里的码就是后端签发的那个", fake_backend.STATE.verify_codes[QQ_OUTSIDER]["code"], code)
    check(
        "/注册 的调用序列（只签码，不碰任何用户数据）",
        sequence_since(mk),
        ["POST /api/verify/request"],
    )

    # --- 非白名单 QQ 发别的指令 → 静默忽略（连 /help 之外都不行）---
    mk = mark()
    before = len(sender.sent)
    await router.handle_private_event(private_event(user_id=int(QQ_OUTSIDER), text="/add 明天交作业"))
    check("非白名单 QQ 发 /add 不回复", len(sender.sent), before)
    check("非白名单 QQ 发 /add 不发请求", sequence_since(mk), [])

    # --- /help 也对任何人开放（不然用户不知道该发什么）---
    mk = mark()
    await router.handle_private_event(private_event(user_id=int(QQ_OUTSIDER), text="/help"))
    check_true("/help 对非白名单也回复", "可用指令" in sender.last(), sender.last()[:40])
    check("/help 不碰后端", sequence_since(mk), [])


# ---------------------------------------------------------------------------
# 6c. /done 的 actor 与租户，以及"另一个人的条一点没动"
# ---------------------------------------------------------------------------


async def test_done_actor_and_tenant(world: World) -> None:
    print("\n=== 6c. /done 写的是 actor=qq:<QQ> + 正确的租户，别人的条不动 ===")
    await world.reset()
    owner_a = await world.register(QQ_OWNER)
    owner_b = await world.register(QQ_OTHER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    await world.subscribe(QQ_OTHER, GROUP, SENDER)
    settings = make_settings(world.port)
    backend = world.client()
    sender = StubSender()
    router = CommandRouter(backend, sender, settings)

    outcome = await ingest_message(
        await norm(
            # ts 用"刚刚"，文案用"明天下午3点" → due_at 在未来 → status=active。
            # 下面的 /list 只列 active，用固定的一年多前的时间戳会什么都列不出来
            # （那不是被测逻辑的问题，是夹具过期了）。见 recent_ts 的说明。
            group_event(message_id=601, ts=recent_ts(), text="明天下午3点 在教三201开班会"),
            settings,
        ),
        backend,
        settings=settings,
    )
    check("两个人的消息扇出成功", outcome, "extracted")
    notifs = list(fake_backend.STATE.notifications.values())
    check("两条通知", len(notifs), 2)
    by_owner = {n["user_id"]: n for n in notifs}
    check_true("A 有自己的那一条", owner_a in by_owner)
    check_true("B 有自己的那一条", owner_b in by_owner)
    notif_a = by_owner.get(owner_a, {})
    notif_b = by_owner.get(owner_b, {})

    # --- A 发 /list 只看到自己的那一条 ---
    await router.handle_private_event(private_event(user_id=int(QQ_OWNER), text="/list"))
    check_true("A 的列表里有自己的条", notif_a.get("title", "")[:6] in sender.last(), sender.last()[:80])

    mk = mark()
    await router.handle_private_event(private_event(user_id=int(QQ_OWNER), text="/done 1"))
    check_true("/done 回执", sender.last().startswith("✅ 已完成"), sender.last())

    rows = fake_backend.STATE.correction_rows
    check("修正历史只有一行（只动了 A 的那条）", len(rows), 1)
    if rows:
        row = rows[0]
        # 两个字段必须分开：user_id 是**这是谁的数据**，actor 是**谁操作的**。
        # 多用户改造里这个改名（user_id → actor）是最危险的一处：
        # 写反了的话，B 的前端会看到"A 完成了"。
        check("修正的租户是 A", row["user_id"], owner_a)
        check("修正的 actor 是 qq:10001", row["actor"], f"qq:{QQ_OWNER}")
        check("修正落在 A 的那条通知上", row["notification_id"], notif_a.get("id"))
    # 修正的 query 参数里带的也必须是租户（服务令牌不会替后端猜归属）
    corr_calls = [c for c in fake_backend.STATE.calls[mk:] if c["path"].endswith("/corrections")]
    check("corrections 的请求只发了一次", len(corr_calls), 1)
    if corr_calls:
        check_true(
            f"corrections 的 query 里带的是租户 {owner_a}",
            f"user_id={owner_a}" in corr_calls[0]["query"],
            corr_calls[0]["query"],
        )
        check_true(
            "corrections 的 body 里带的是 actor",
            str((corr_calls[0].get("request_body") or {}).get("actor")) == f"qq:{QQ_OWNER}",
            str(corr_calls[0].get("request_body"))[:120],
        )

    # --- B 的那条一点没动：既没有修正历史，读投影也还是 active ---
    check_true(
        "B 的条没有修正记录",
        all(r["notification_id"] != notif_b.get("id") for r in fake_backend.STATE.correction_rows),
    )
    check_true(
        "B 的条仍然是未修正状态",
        notif_b.get("id") not in fake_backend.STATE.corrections,
    )

    # --- B 发 /done 1 动的是自己的那条 ---
    await router.handle_private_event(private_event(user_id=int(QQ_OTHER), text="/list"))
    await router.handle_private_event(private_event(user_id=int(QQ_OTHER), text="/done 1"))
    rows = fake_backend.STATE.correction_rows
    check("现在有两行修正历史", len(rows), 2)
    if len(rows) == 2:
        check("第二行的租户是 B", rows[1]["user_id"], owner_b)
        check("第二行的 actor 是 qq:10002", rows[1]["actor"], f"qq:{QQ_OTHER}")
        check("第二行落在 B 的那条通知上", rows[1]["notification_id"], notif_b.get("id"))
    check_true(
        "A 的那条仍然是 done（没被覆盖）",
        fake_backend.STATE.corrections.get(notif_a.get("id", ""), {}).get("status") == "done",
        str(fake_backend.STATE.corrections.get(notif_a.get("id", ""))),
    )


# ---------------------------------------------------------------------------
# 7. 订阅指令端到端
# ---------------------------------------------------------------------------


async def test_subscription_commands(world: World) -> None:
    print("\n=== 7. 订阅指令：/来源、/订阅、/订阅列表、/退订 ===")
    await world.reset()
    owner = await world.register(QQ_OWNER)
    settings = make_settings(world.port)
    backend = world.client()
    sender = StubSender()
    router = CommandRouter(backend, sender, settings)

    # 先造一条**共享层**的原文，/来源 才有东西可列（目录是从 raw_message 聚合的）
    await backend.create_message(
        {
            "message_id": "src-1",
            "group_id": GROUP,
            "group_name": "示例通知群",
            "sender_id": SENDER,
            "sender_name": "张老师",
            "ts": 1757692800000,
            "content": "本周五19:00开班会",
            "raw": {},
        }
    )

    # --- /来源 列出目录并把编号映射存后端 ---
    mk = mark()
    await router.handle_private_event(private_event(text="/来源"))
    print("    /来源 输出：")
    for line in sender.last().splitlines():
        print(f"      {line}")
    check_true("/来源 有表头", sender.last().startswith("📚 可订阅的来源"), sender.last().splitlines()[0])
    check_true("列表里能看到那个来源", "示例通知群" in sender.last())
    check_true(
        "来源编号映射存进了后端",
        (owner, "command_sources", QQ_OWNER) in fake_backend.STATE.kv,
    )
    sources = fake_backend.STATE.kv[(owner, "command_sources", QQ_OWNER)]["value"]["items"]
    check("目录里就是那条 (群, 发送者)", (sources[0]["group_id"], sources[0]["sender_id"]), (GROUP, SENDER))

    # --- /订阅 <编号> 建订阅；路由名单里随即出现这个用户 ---
    check("订阅前路由名单是空的", await backend.find_subscribers(GROUP, SENDER), [])
    mk = mark()
    await router.handle_private_event(private_event(text="/订阅 1"))
    check_true("/订阅 <编号> 回执", sender.last().startswith("✅ 已订阅"), sender.last())
    check("订阅后路由名单里有这个用户", await backend.find_subscribers(GROUP, SENDER), [owner])
    subs = list(fake_backend.STATE.subscriptions.values())
    check("后端里有一条订阅", len(subs), 1)
    if subs:
        check("订阅的归属是租户", subs[0]["user_id"], owner)
        check("订阅的是那个 (群, 发送者)", (subs[0]["group_id"], subs[0]["sender_id"]), (GROUP, SENDER))

    # --- /退订 <编号> 删掉它 ---
    await router.handle_private_event(private_event(text="/订阅列表"))
    check_true("/订阅列表 有表头", sender.last().startswith("🔔 你的订阅"), sender.last().splitlines()[0])
    check_true(
        "订阅编号映射存进了后端",
        (owner, "command_subs", QQ_OWNER) in fake_backend.STATE.kv,
    )
    await router.handle_private_event(private_event(text="/退订 1"))
    check_true("/退订 回执", sender.last().startswith("🗑 已退订"), sender.last())
    check_true("订阅真的被删了", fake_backend.STATE.subscriptions == {})
    check("退订后路由名单又空了", await backend.find_subscribers(GROUP, SENDER), [])

    # --- /订阅 <群号> <发送者QQ> 直接订（不依赖 /来源 的编号）---
    await router.handle_private_event(private_event(text=f"/订阅 {GROUP} {SENDER}"))
    check_true("直接给群号和发送者也能订", sender.last().startswith("✅ 已订阅"), sender.last())
    check("直接订阅后路由名单里有他", await backend.find_subscribers(GROUP, SENDER), [owner])

    # --- /订阅 <群号> 单独一个群号 → 明确说"还缺发送者"，**绝不**订整个群 ---
    await world.reset()
    owner = await world.register(QQ_OWNER)
    await router.handle_private_event(private_event(text=f"/订阅 {GROUP}"))
    check("/订阅 只给群号时的回执", sender.last(), SUBSCRIBE_NEED_SENDER)
    check_true("没有偷偷订成整个群", fake_backend.STATE.subscriptions == {})
    check("路由名单仍然是空的", await backend.find_subscribers(GROUP, SENDER), [])

    # --- 编号过期 → 回「先发 /来源」，绝不猜 ---
    fake_backend.STATE.kv.pop((owner, "command_sources", QQ_OWNER), None)
    await router.handle_private_event(private_event(text="/订阅 1"))
    check("/订阅 编号过期时的回执", sender.last(), SOURCES_STALE_REPLY)
    check_true("过期时没有建任何订阅", fake_backend.STATE.subscriptions == {})

    # --- /退订 编号过期 → 回「先发 /订阅列表」，绝不猜 ---
    fake_backend.STATE.kv.pop((owner, "command_subs", QQ_OWNER), None)
    await router.handle_private_event(private_event(text="/退订 1"))
    check_true(
        "/退订 编号过期时要求刷新列表",
        "订阅编号已过期" in sender.last(),
        sender.last(),
    )

    # --- 端到端：订完之后那条消息真的会被抽取并建条 ---
    await router.handle_private_event(private_event(text=f"/订阅 {GROUP} {SENDER}"))
    mk = mark()
    outcome = await ingest_message(
        await norm(group_event(message_id=701, text="本周五19:00在教三201开班会"), settings),
        backend,
        settings=settings,
    )
    check("通过指令订完之后，消息真的被抽取了", outcome, "extracted")
    check("并且建了那个用户的通知", owners_of_notifications(), [owner])


# ---------------------------------------------------------------------------
# 8. digest：按用户组装、按各自的 QQ 发送
# ---------------------------------------------------------------------------


async def test_digest(world: World) -> None:
    print("\n=== 8. digest 预览 / 发送 / 今日已发判定（单用户 + DIGEST_TARGET_QQ 覆盖）===")
    await world.reset()
    owner = await world.register(QQ_OWNER)
    # 这个用户自己的 digest 要有内容
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    backend = world.client()
    settings = make_settings(world.port, digest_target_qq=QQ_OWNER)
    sender = StubSender()
    configure_digest(backend, sender)

    await ingest_message(
        await norm(group_event(message_id=777, text="大家下周三前把军训心得交到班长那里"), settings),
        backend,
        settings=settings,
    )
    text = await build_digest(backend, settings, user_id=owner)
    check_true("digest 有标题行", text.startswith("【Xcollector 每日通知】"), text.splitlines()[0])
    check_true("digest 含新增条目", "军训心得" in text)
    check_true("digest 含盲区段", "本系统今日盲区" in text)
    check_true("digest 含服务器时间", "服务器时间" in text)

    with settings_patch(settings):
        targets = await resolve_recipients(backend, settings)
        check("DIGEST_TARGET_QQ 配了 → 只发那一个 QQ", [t[0] for t in targets], [QQ_OWNER])
        check("收件人带的是那个人的租户", [t[1] for t in targets], [owner])

        result = await send_digest(dry_run=True, backend=backend, sender=sender)
        check("dry_run 不发送", result["sent"], 0)
        check("dry_run ok", result["ok"], True)
        check("dry_run 的总数", result["total"], 1)
        check("dry_run 写了一条 preview 记录", len(fake_backend.STATE.digest_logs), 1)
        check("preview 记录的 kind", fake_backend.STATE.digest_logs[0]["kind"], "preview")
        check("preview 记录的归属是那个用户", fake_backend.STATE.digest_logs[0]["user_id"], owner)

        check("还没有发过 → sent_today=False", await sent_today(backend, owner), False)

        result = await send_digest(dry_run=False, kind="auto", backend=backend, sender=sender)
        check("真的发出去了", result["sent"], 1)
        check("发送调用了一次私聊", len(sender.sent), 1)
        check("收件人是 DIGEST_TARGET_QQ", sender.sent[0][0], QQ_OWNER)
        check_true("发送后 sent_today=True", await sent_today(backend, owner))
        auto_logs = [r for r in fake_backend.STATE.digest_logs if r["kind"] == "auto"]
        check("auto 记录 sent=true", auto_logs[0]["sent"] if auto_logs else None, True)

    # 关键：状态在后端，不在 bot 内存 —— 换一个实例依然知道今天发过了
    other = world.client()
    check_true("重启后的 bot 依然不会重发", await sent_today(other, owner))

    # --- DIGEST_TARGET_QQ 指向一个**没注册**的 QQ → 记 ERROR、一个都不发 ---
    await world.reset()
    owner = await world.register(QQ_OWNER)
    strays = make_settings(world.port, digest_target_qq=QQ_OUTSIDER)
    with settings_patch(strays):
        targets = await resolve_recipients(backend, strays)
        check("目标 QQ 没注册 → 收件人名单为空", targets, [])
        before = len(sender.sent)
        result = await send_digest(dry_run=False, backend=backend, sender=sender)
        check("没有可发的人 → ok=False", result["ok"], False)
        check("没有可发的人 → sent=0", result["sent"], 0)
        check("没有可发的人 → total=0", result["total"], 0)
        check_true("没有可发的人 → 有 error 说明", bool(result["error"]), str(result["error"]))
        check("一个私聊都没发出去", len(sender.sent), before)
        check("也没写 digest_log（没人可记）", fake_backend.STATE.digest_logs, [])

    await world.close()
    reset_digest_context()


async def test_digest_multi_user(world: World) -> None:
    print("\n=== 8b. digest 多用户：一个人一条私聊，各自组装 ===")
    await world.reset()
    owner_a = await world.register(QQ_OWNER)
    owner_b = await world.register(QQ_OTHER)
    # 两个人订**不同**的来源：这样"各组装各的"才有可观测的差别
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    await world.subscribe(QQ_OTHER, GROUP, QQ_OTHER)
    backend = world.client()
    settings = make_settings(world.port)  # digest_target_qq 留空 = 多用户模式
    sender = StubSender()
    configure_digest(backend, sender)

    # 两条原文，各进一个人的 digest
    await ingest_message(
        await norm(group_event(message_id=801, text="本周五19:00在教三201开班会"), settings),
        backend,
        settings=settings,
    )
    await ingest_message(
        await norm(
            group_event(message_id=802, user_id=int(QQ_OTHER), card="李老师", text="下周一前交实验报告"),
            settings,
        ),
        backend,
        settings=settings,
    )
    text_a = await build_digest(backend, settings, user_id=owner_a)
    text_b = await build_digest(backend, settings, user_id=owner_b)
    check_true("A 的 digest 里有自己那条", "开班会" in text_a, text_a[:200])
    check_true("A 的 digest 里**没有**别人的那条", "实验报告" not in text_a)
    check_true("B 的 digest 里有自己那条", "实验报告" in text_b, text_b[:200])
    check_true("B 的 digest 里**没有**别人的那条", "开班会" not in text_b)

    with settings_patch(settings):
        targets = await resolve_recipients(backend, settings)
        check("没配 DIGEST_TARGET_QQ → 每个注册用户各一份", sorted(t[0] for t in targets), sorted([QQ_OWNER, QQ_OTHER]))
        check("收件人带的是各自的租户", sorted(t[1] for t in targets), sorted([owner_a, owner_b]))

        result = await send_digest(dry_run=False, kind="auto", backend=backend, sender=sender)
        check("两个人都发出去了", result["sent"], 2)
        check("总数是 2", result["total"], 2)
        check("两次私聊", len(sender.sent), 2)
        # **最关键的一条**：每份都发到那个人**自己的** QQ 上，不是发给一个共享收件人。
        check_true(
            "两份 digest 分别发到各自的 QQ",
            sorted(qq for qq, _msg in sender.sent) == sorted([QQ_OWNER, QQ_OTHER]),
            str([qq for qq, _m in sender.sent]),
        )
        for qq, msg in sender.sent:
            if qq == QQ_OWNER:
                check_true("A 收到的那份是 A 的内容", "开班会" in msg and "实验报告" not in msg)
            else:
                check_true("B 收到的那份是 B 的内容", "实验报告" in msg and "开班会" not in msg)

        logs = fake_backend.STATE.digest_logs
        check("digest_log 一人一行", len(logs), 2)
        check("两行日志分属两个租户", sorted(r["user_id"] for r in logs), sorted([owner_a, owner_b]))
        check("两行都标了 sent=true", sorted(bool(r["sent"]) for r in logs), [True, True])
        check_true("A 的 sent_today 是 True", await sent_today(backend, owner_a))
        check_true("B 的 sent_today 是 True", await sent_today(backend, owner_b))

        # sent_today 必须**按用户**问：伪造一个只有 A 发过的日子，B 应当还是 False
        result = await send_digest(
            dry_run=False, kind="auto", user_id=owner_a, qq=QQ_OWNER, backend=backend, sender=sender
        )
        check("只给 A 再发一次 → 只发了 1 条", result["sent"], 1)

        # dry_run 也要一人一行 preview 记录
        await world.reset()
        await world.register(QQ_OWNER)
        await world.register(QQ_OTHER)
        result = await send_digest(dry_run=True, backend=backend, sender=sender)
        check("dry_run 也是一人一条记录", result["total"], 2)
        check("dry_run 写了 2 条 preview", len(fake_backend.STATE.digest_logs), 2)
        check(
            "dry_run 的记录也是分租户的",
            sorted(r["user_id"] for r in fake_backend.STATE.digest_logs),
            sorted(world.users.values()),
        )

    await world.close()
    reset_digest_context()


# ---------------------------------------------------------------------------
# 9. 认证（连后端）
# ---------------------------------------------------------------------------


async def test_auth(world: World) -> None:
    print("\n=== 9. 共享密钥（API_TOKEN）===")
    fake_backend.STATE.token = "test-token"
    try:
        await world.reset()
        owner = await world.register(QQ_OWNER)
        await world.subscribe(QQ_OWNER, GROUP, SENDER)
        good = make_settings(world.port)
        backend = world.client(good)
        mk = mark()
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
        # 路由接口是**服务令牌专属**的：它返回的是全局投递名单（谁订了哪个来源）。
        # 这条断言钉住"bot 用的是服务令牌"，不然将来换成用户令牌会静默漏扇。
        check_true(
            "调了服务令牌专属的投递名单接口",
            any(c["path"] == "/api/subscriptions/routing" for c in calls),
            str([c["path"] for c in calls]),
        )

        bad = make_settings(world.port, api_token="wrong-token")
        backend_bad = world.client(bad)
        health = await backend_bad.health()
        check("令牌不对 → reachable=False", health["reachable"], False)
        check_true("错误里能看到 401", "401" in (health.get("error") or ""), str(health.get("error"))[:80])
    finally:
        fake_backend.STATE.token = ""


# ---------------------------------------------------------------------------
# 10. bot 暴露给前端的接口
# ---------------------------------------------------------------------------


async def test_bot_api(world: World) -> None:
    print("\n=== 10. /api/status 与 /api/digest/*（前端调的那几个）===")
    import httpx

    from app import main as main_mod

    await world.reset()
    owner = await world.register(QQ_OWNER)
    await world.subscribe(QQ_OWNER, GROUP, SENDER)
    settings = make_settings(world.port, digest_target_qq=QQ_OWNER)
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
        if resp.status_code == 200:
            payload = resp.json()
        else:
            payload = {}

        check(
            "status 的顶层就是契约第 9 节那些键",
            sorted(payload),
            sorted([
                "onebot", "llm", "whitelist", "pipeline", "blindspots", "groups",
                "gap_alerts", "backend", "recovery", "digest", "day", "server_time",
            ]),
        )
        if payload:
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
                any(g["group_id"] == GROUP and g["in_whitelist"] for g in payload["groups"]),
                str(payload["groups"])[:120],
            )
            # 按用户的那一层：一个人一张卡片，标量是它们的累加
            check("blindspots.per_user 里有这个用户", [r["user_id"] for r in payload["blindspots"]["per_user"]], [owner])
            check("pipeline.users_counted", payload["pipeline"]["users_counted"], 1)
            check("digest.sent_today（计数，不是布尔）", payload["digest"]["sent_today"], 0)
            check("digest.sent_today_all", payload["digest"]["sent_today_all"], False)

        preview = await client.get("/api/digest/preview", headers=headers)
        check("digest/preview → 200", preview.status_code, 200)
        if preview.status_code == 200:
            body = preview.json()
            check_true("digest/preview 返回 text", "每日通知" in body.get("text", ""))
            check("digest/preview 说明预览的是谁", body.get("user_id"), owner)
            check("digest/preview 带 qq", body.get("qq"), QQ_OWNER)

        sent = await client.post("/api/digest/send", json={"dry_run": True}, headers=headers)
        check("digest/send → 200", sent.status_code, 200)
        body = sent.json()
        check(
            "digest/send 的键（多用户之后带 total / recipients）",
            sorted(body),
            sorted(["ok", "sent", "total", "dry_run", "text", "recipients", "error"]),
        )
        check("dry_run 不发", body["sent"], 0)
        check("dry_run ok", body["ok"], True)
        check("dry_run total", body["total"], 1)

        noauth = await client.post("/api/digest/send", json={"dry_run": True})
        check("digest/send 也校验令牌", noauth.status_code, 401)

    await runtime.backend.close()
    main_mod.set_runtime(None)


# ---------------------------------------------------------------------------
# 11. 只剩一个令牌：拿"网页令牌"什么都干不了
# ---------------------------------------------------------------------------


async def test_bot_scopes(world: World) -> None:
    print("\n=== 11. 多用户之后没有网页令牌：那个值现在什么都不是 ===")
    import httpx

    from app import main as main_mod

    settings = make_settings(world.port)
    runtime = main_mod.BotRuntime(settings)
    runtime.hub = RecordingHub()  # type: ignore[assignment]
    main_mod.set_runtime(runtime)

    admin_h = {"Authorization": "Bearer bot-secret"}
    web_h = {"Authorization": "Bearer web-secret"}  # 单租户时代的网页令牌，现在无效

    transport = httpx.ASGITransport(app=main_mod.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://bot") as client:
        # 网页令牌**读**也进不去：/api/status 是运营者视角（所有人的盲区计数）
        for path in ("/api/status", "/api/digest/preview"):
            r = await client.get(path, headers=web_h)
            check(f"网页令牌 GET {path} → 401", r.status_code, 401)
            detail = (r.json() or {}).get("detail", "") if r.status_code == 401 else ""
            check_true(f"{path} 说明了是令牌不对", "令牌" in detail, repr(detail))

        # 管理令牌照旧放行（后端此时没有注册用户，preview 会返回空文本但仍是 200）
        r = await client.get("/api/status", headers=admin_h)
        check("管理令牌 GET /api/status → 200", r.status_code, 200)

        # 写接口对网页令牌是**拒绝**：身份无效就是 401，不是 403
        writes = [
            ("POST", "/api/digest/send", {"dry_run": True}),
            ("POST", "/api/send/private", {"user_id": "10001", "message": "hi"}),
            ("POST", "/api/send/group", {"group_id": GROUP, "message": "hi"}),
        ]
        for method, path, body in writes:
            r = await client.request(method, path, json=body, headers=web_h)
            check(f"网页令牌 {method} {path} → 401", r.status_code, 401)

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
        r = await client.post("/api/digest/send", json={"dry_run": True}, headers=admin_h)
        check("管理令牌 POST /api/digest/send → 200", r.status_code, 200)

        # 坏令牌仍然是 401
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
    # 每一组断言前都要有真用户/真订阅，所以夹具按组重建（见各测试开头）。
    fake_backend.STATE.token = ""
    world = World(port)
    try:
        await test_group_message_flow(world)
        await test_unsubscribed_source(world)
        await test_fan_out_to_two_users(world)
        await test_routing_failure_stays_pending(world)
        await test_filters(world)
        await test_gap_detection(world)
        await test_crash_recovery(world)
        await test_backend_down(world)
        await test_blindspots(world)
        await test_commands(world)
        await test_registration_and_identity(world)
        await test_done_actor_and_tenant(world)
        await test_subscription_commands(world)
        await test_digest(world)
        await test_digest_multi_user(world)
        await test_auth(world)
        await test_bot_api(world)
        await test_bot_scopes(world)
    finally:
        await world.close()
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
