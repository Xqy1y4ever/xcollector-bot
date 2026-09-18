"""FastAPI 应用入口 + 生命周期。

    uvicorn app.main:app --port 8082
或：
    python -m app.main

这个进程同时干两件事，但它们是**两个不同的端口**，别搞混：
  - BOT_LISTEN_PORT（默认 8082）：本服务的 HTTP API，给前端 / 后端调；
  - ONEBOT_LISTEN_PORT（默认 8081，仅 server 模式）：给 NapCat 的反向 WS 用。

消息处理现在是**两段式**（见 MessagePipeline）：
  1. 收到事件 → 归一化 → 群白名单 → **写前日志** POST /api/messages；
  2. 拿到 id 之后的重活（附件、抽取、建通知、统计）交给一个后台 worker。

第一段必须快且先做，因为 QQ 群消息是唯一不可再生的资产；第二段慢且可以重来，
因为原文已经在后端里了（`state=pending` 就是它的恢复队列）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import dataclass
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, WebSocket
from pydantic import BaseModel, field_validator

from . import __version__
from .auth import Scope, require_token, require_write
from .llm.target import target_from_settings
from .backend_client import BackendClient, BackendError
from .commands import CommandRouter
from .config import Settings, get_settings
from .logging_setup import setup_logging
from .normalize import MessageNormalizer, NormalizedMessage, message_kind
from .onebot import OneBotHub, OneBotNotConnected
from .onebot.hub import interpret_send_result
from .pipeline.digest import build_digest, configure_digest, digest_loop, send_digest
from .pipeline.runner import (
    close_media_client,
    finish_message,
    ingest_message,
    resume_pending,
    write_ahead,
)
from .pipeline.trace import elapsed_ms, log_received, log_stage
from .pipeline.watchdog import silence_loop, startup_gap_check
from .utils import local_day, now_ms, truncate

setup_logging()
logger = logging.getLogger("xcollector.bot")
_settings = get_settings()

# /api/send/* 的长度上限。超长消息 NapCat 会直接报错，
# 与其让调用方去猜为什么发不出去，不如截断后如实告知（后缀就是告知）。
MESSAGE_MAX_CHARS = 1500
TRUNCATE_SUFFIX = "…（已截断）"

# 第二段的重活队列上限。队列满时丢**最新**的那条并记 ERROR ——
# 它不会丢数据（原文已经在后端里，状态停留在 pending），
# 下一次 resume_pending() 会把它捡回来。
QUEUE_MAX_SIZE = 2000

# 崩溃恢复：每轮最多补多少条、多久扫一次
RECOVERY_SWEEP_SECONDS = 60.0

# 盲区计数的时间窗口（和后端原来的 `_blindspots()` 保持一致）
UNPARSED_WINDOW_DAYS = 7


# ---------------------------------------------------------------------------
# 两段式消息流水线
# ---------------------------------------------------------------------------


@dataclass
class QueuedMessage:
    """已经落库、等着做重活的一条消息。"""

    raw_id: str
    doc: dict
    is_new: bool


class MessagePipeline:
    """接收 → 写前日志 → 队列 → 附件/抽取/建条。

    **为什么不再攒批**：改造前用 200ms 窗口把连续几条消息合成一个请求发走。
    现在每条消息都要下载附件、调 LLM、写好几次后端 —— 根本没法批处理，
    那个缓冲只会变成一个"进程被 kill 时缓冲区里的原始消息直接消失"的窗口。
    所以缓冲被删掉了：收到就立刻落库。

    **为什么要队列**：写前日志之后还有很长的重活（LLM 可能跑 60 秒）。
    队列把"接收"和"处理"解耦，且 worker 只有**一个**：
    群状态 upsert 必须按消息时间顺序执行，否则 last_msg_ts 会来回跳，
    缺口检测（runner._maybe_gap_alert）就会开始报假警。
    """

    def __init__(self, backend: BackendClient, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.backend = backend
        self._queue: asyncio.Queue[QueuedMessage] = asyncio.Queue(QUEUE_MAX_SIZE)
        self._worker: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None
        self.dropped = 0
        self.processed = 0

    async def start(self) -> None:
        self._worker = asyncio.create_task(self._work_loop())
        self._retry_task = asyncio.create_task(self._retry_loop())

    @property
    def depth(self) -> int:
        """第二段队列里还压着多少条（给 /api/status 用）。"""
        return self._queue.qsize()

    async def stop(self) -> None:
        for task in (self._worker, self._retry_task):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        self._worker = None
        self._retry_task = None
        await close_media_client()

    async def submit(self, msg: NormalizedMessage) -> str:
        """第一段：群白名单 + 写前日志。返回结果字符串（也是一行日志里的 `结果=`）。"""
        wa = await write_ahead(msg, self.backend, self.settings)
        if wa.outcome != "ok":
            return wa.outcome
        assert wa.raw_id
        try:
            self._queue.put_nowait(QueuedMessage(wa.raw_id, wa.doc, wa.is_new))
        except asyncio.QueueFull:
            # 不丢数据：原文已经在后端里、状态停在 pending，
            # 下一轮 resume_pending() 会把它捡回来继续做。
            self.dropped += 1
            logger.error(
                "重活队列已满，暂缓处理 raw=%s（原文已落库，等待崩溃恢复补处理；累计 %d 条）",
                wa.raw_id,
                self.dropped,
            )
            return "deferred"
        return "queued"

    # ---------------- 内部 ----------------

    async def _work_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                await finish_message(
                    item.raw_id,
                    item.doc,
                    self.backend,
                    settings=self.settings,
                    is_new=item.is_new,
                )
                self.processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 单条失败不能拖垮 worker
                logger.exception("处理消息失败 raw=%s: %s", item.raw_id, exc)

    async def _retry_loop(self) -> None:
        """补写"连原文都没写进后端"的那些消息。

        这一类是唯一真正可能丢的东西（后端 4xx / 一直不可达），所以：
          - 立刻打一行带 `待重试=是` 的 ERROR（绝不静默）；
          - 队列计数暴露在 /api/status 的 `pipeline.pending_retry` 上；
          - 每 PENDING_RETRY_INTERVAL 秒重放一次，超过次数后记 ERROR 放弃。
        已经落库、只是没处理完的消息**不走这里**，走 resume_pending()。
        """
        interval = max(5.0, float(self.settings.pending_retry_interval))
        max_attempts = max(1, int(self.settings.pending_retry_max_attempts))
        while True:
            await asyncio.sleep(interval)
            for item in self.backend.pending.items():
                self.backend.pending.remove(item)
                item.attempts += 1
                if item.attempts > max_attempts:
                    logger.error(
                        "待重试写入已放弃（试了 %d 次）：群=%s msg_id=%s | 原因=%s",
                        item.attempts - 1,
                        item.message.get("group_id"),
                        item.message.get("message_id"),
                        item.note,
                    )
                    continue
                try:
                    outcome = await ingest_message(
                        item.message, self.backend, settings=self.settings, retry=True
                    )
                except Exception as exc:
                    logger.exception("补写异常：%s", exc)
                    outcome = "error"
                if outcome != "error":
                    logger.info(
                        "补写成功（第 %d 次）：msg_id=%s → %s",
                        item.attempts,
                        item.message.get("message_id"),
                        outcome,
                    )
                    continue
                # 又失败了：runner 已经重新入队，把尝试次数接上，避免无限重试
                for fresh in self.backend.pending.items():
                    if fresh.message.get("message_id") == item.message.get("message_id"):
                        fresh.attempts = item.attempts
                        break


# ---------------------------------------------------------------------------
# 反向 WS 专用服务（server 模式）
# ---------------------------------------------------------------------------


def build_onebot_ws_app() -> FastAPI:
    """反向 WS 的专用小应用：只有一条路由，别的什么都不暴露。

    为什么要单独起一个 server 而不是挂在主应用上：
    `ONEBOT_LISTEN_PORT`(8081) 和 `BOT_LISTEN_PORT`(8082) 是**两个不同的端口**，
    一个给 NapCat 连、一个给外部调。uvicorn 一个实例只监听一个端口，
    所以 server 模式下额外起一个只服务 WS 的实例。
    两个 server 跑在**同一个事件循环**里 —— 这一点是必须的：
    hub 的连接对象是 asyncio 原语，跨线程/跨循环用会直接坏掉。
    """
    ws_app = FastAPI(
        title="Xcollector bot · OneBot reverse WS",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @ws_app.websocket(_settings.onebot_listen_path)
    async def onebot_reverse_ws(websocket: WebSocket) -> None:
        await get_runtime().hub.attach_server_ws(websocket)

    return ws_app


class ReverseWsServer:
    """内嵌的第二个 uvicorn 实例，只监听 OneBot 反向 WS 的端口。"""

    READY_TIMEOUT = 5.0

    def __init__(self, settings: Settings):
        self.settings = settings
        self._server = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        import uvicorn

        config = uvicorn.Config(
            build_onebot_ws_app(),
            host=self.settings.onebot_listen_host,
            port=self.settings.onebot_listen_port,
            log_level="warning",
            access_log=False,
            lifespan="off",
        )
        server = uvicorn.Server(config)
        # 关键：这个内嵌实例绝不能去抢进程的信号处理。
        # 否则 Ctrl+C 只会把它自己关掉，外层 uvicorn 还以为一切正常。
        # 把实例属性盖成 nullcontext 就等于"不装信号处理器"，比改 uvicorn 内部稳。
        server.capture_signals = nullcontext  # type: ignore[method-assign]
        self._server = server
        self._task = asyncio.create_task(server.serve())

        waited = 0.0
        while not server.started and waited < self.READY_TIMEOUT:
            if self._task.done():
                exc = self._task.exception()
                raise RuntimeError(f"反向 WS 服务启动失败：{exc!r}")
            await asyncio.sleep(0.05)
            waited += 0.05

        if not server.started:
            raise RuntimeError(
                f"反向 WS 服务在 {self.READY_TIMEOUT:.0f}s 内没有起来，"
                f"检查端口 {self.settings.onebot_listen_port} 是否被占用"
            )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            with suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                await asyncio.wait_for(self._task, timeout=5.0)
            self._task = None


# ---------------------------------------------------------------------------
# 运行时
# ---------------------------------------------------------------------------


class BotRuntime:
    """把 hub / backend / 归一化 / 流水线 / 指令串起来。

    放在一个类里而不是散在 lifespan 的闭包里，是为了让
    "谁依赖谁"一眼可见，也方便把假 hub / 假 backend 塞进来做自检。
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        # 刻意不用模块级的 get_hub() 单例：hub 必须用"这份 settings"，
        # 否则同一进程里换配置再建一个 runtime（测试、自检工具都会这么做）
        # 会拿到上一个 hub，连到上一个地址去。
        self.hub = OneBotHub(self.settings)
        self.backend = BackendClient(self.settings)
        self.normalizer = MessageNormalizer(self.settings)
        self.router = CommandRouter(self.backend, self.hub, self.settings)
        self.pipeline = MessagePipeline(self.backend, self.settings)
        self.ws_server = ReverseWsServer(self.settings)
        self._tasks: list[asyncio.Task] = []
        self.last_recovery: dict = {"at": None, "count": 0, "error": None}

    async def start(self) -> None:
        settings = self.settings
        logger.info(
            "OneBot 模式=%s，目标=%s",
            settings.onebot_mode,
            settings.onebot_ws_url
            if settings.onebot_mode == "client"
            else settings.onebot_server_target,
        )
        logger.info(
            "指令白名单：%s",
            ", ".join(f"{v}({k})" for k, v in settings.command_whitelist_map.items())
            or "（为空：任何人都不能发指令）",
        )
        logger.info(
            "群白名单：%s",
            ", ".join(f"{v}({k})" for k, v in settings.group_whitelist_map.items())
            or "**为空 —— 一个群都不会处理**",
        )
        logger.info(
            "发送者白名单：mode=%s %s",
            settings.sender_whitelist_mode,
            ", ".join(f"{v}({k})" for k, v in settings.sender_whitelist_map.items())
            or ("（mode=off：不限发送者）" if settings.sender_whitelist_mode == "off"
                else "**为空 —— 一个发送者都不会处理**"),
        )
        if not settings.whitelist_ready:
            # 这条必须显眼：白名单是 fail-closed 的，没配好 = bot 连上了但什么都不干，
            # 表现出来只是"收不到消息"，很容易被当成连不上 NapCat 去查错方向。
            logger.warning(
                "白名单没配好：bot 会**忽略所有消息**。请在 .env 里设置 "
                "GROUP_WHITELIST（要处理的群）和 SENDER_WHITELIST（发布通知的人），"
                "格式 id:名称,id:名称。只想先跑通可以临时把 SENDER_WHITELIST_MODE 设成 off。"
            )

        primary = target_from_settings(settings, "primary")
        logger.info("抽取器=%s（主模型=%s）", settings.extractor, primary.describe())
        if settings.cross_check_enabled:
            logger.info("交叉验证模型=%s", target_from_settings(settings, "secondary").describe())
        logger.info("后端地址：%s", settings.backend_base)

        # OneBot 的**生效值**（token 只报有没有，不报内容）。
        # 为什么要专门打这一行：部署时最常见的坑是"改了 .env 但容器没重建"——
        # 容器的环境变量在**创建那一刻**就固定了，`docker compose restart` 不会重读 .env。
        # 那时这里显示的还是旧地址，一眼就能看出来，不用去猜"是不是程序没读 .env"。
        if settings.onebot_mode == "client":
            logger.info(
                "OneBot：mode=client → 连接 %s（access_token %s）",
                settings.onebot_ws_url,
                "已配置" if settings.onebot_access_token else "**未配置**",
            )
        else:
            logger.info(
                "OneBot：mode=server → 监听 %s:%s%s（access_token %s）",
                settings.onebot_listen_host,
                settings.onebot_listen_port,
                settings.onebot_listen_path,
                "已配置" if settings.onebot_access_token else "**未配置**",
            )

        if not settings.inbound_token:
            # 这条 WARNING 是刻意留的：/api/send/* 能冒充机器人发言，
            # 忘了配 token 就等于把它暴露给任何能访问这个端口的人。
            logger.warning("BOT_API_TOKEN / API_TOKEN 均为空：/api/* 不校验认证，仅限本地开发使用")
        elif settings.web_scope_separated:
            logger.info(
                "网页令牌已分离：WEB_API_TOKEN 只能看 /api/status 与 /api/digest/preview，"
                "发消息类接口（send/*、digest/send）只认管理令牌"
            )
        else:
            logger.warning(
                "WEB_API_TOKEN 未配置（或与管理令牌相同）：网页那个令牌拥有**完整权限**，"
                "包括以你的身份发 QQ 消息。请单独设一个不同的 WEB_API_TOKEN。"
            )
        if not settings.command_whitelist_map:
            logger.warning("COMMAND_WHITELIST 为空：当前没有任何 QQ 号能发指令")

        configure_digest(self.backend, self.hub)
        self.hub.set_event_handler(self.handle_event)
        await self.hub.start()
        if settings.onebot_mode == "server":
            # 反向 WS 单独占一个端口（NapCat 连这里），和 HTTP API 端口分开
            await self.ws_server.start()
            logger.info(
                "反向 WS 已监听：%s:%s%s（把 NapCat 的反向 WebSocket 指到这里）",
                settings.onebot_listen_host,
                settings.onebot_listen_port,
                settings.onebot_listen_path,
            )
        await self.pipeline.start()
        self._tasks = [
            asyncio.create_task(digest_loop()),
            asyncio.create_task(silence_loop(self.backend, self.settings)),
            asyncio.create_task(self._recovery_loop()),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks = []
        await self.pipeline.stop()
        if self.settings.onebot_mode == "server":
            await self.ws_server.stop()
        await self.hub.stop()
        await self.backend.close()
        logger.info("已关闭")

    # ---------------- 事件 ----------------

    async def handle_event(self, event: dict) -> None:
        """OneBot 事件总入口。"""
        kind = message_kind(event)
        if kind == "group":
            # **先留痕，再干活。** 归一化要查群名（一次网络往返），后面还有附件
            # 下载和模型调用，加起来可能几十秒。如果只在处理完之后打日志，
            # 一条卡住的消息在日志里完全看不到 —— 而这条链路最怕的就是静默。
            #
            # 群不在白名单的降到 DEBUG：那类消息量大且重复，INFO 会把日志刷爆，
            # 而且它们的结局（group_filtered）本来也只值 DEBUG。两边级别一致，
            # 才不会出现"只有收到、没有下文"的困惑行。
            in_group = self.settings.in_group_whitelist(event.get("group_id"))
            log_received(
                event,
                level=logging.INFO if in_group else logging.DEBUG,
                # 用内存里的群名缓存，**不查网络** —— 这行必须在处理之前打出来
                group_name=self.normalizer.groups.name(str(event.get("group_id") or "")),
            )

            started = time.monotonic()
            msg = await self.normalizer.normalize(event, self.hub)
            if msg is None:
                log_stage("归一化", {"message_id": event.get("message_id")}, 结果="已忽略（自己发的/无法解析）")
                return
            log_stage(
                "归一化",
                {"message_id": msg.message_id},
                附件=len(msg.attachments or []) or None,
                合并转发="是" if "[合并转发]" in (msg.text or "") else None,
                耗时=elapsed_ms(started),
            )
            await self.pipeline.submit(msg)
        elif kind == "private":
            await self.router.handle_private_event(event)
        else:
            # discuss（讨论组）等暂不支持。只记 DEBUG，避免刷日志。
            logger.debug("忽略事件类型 message_type=%s", event.get("message_type"))

    # ---------------- 崩溃恢复 ----------------

    async def run_recovery(self) -> int:
        """把后端里停在 pending 的消息捡回来继续处理。"""
        count = await resume_pending(self.backend, self.settings)
        self.last_recovery = {"at": now_ms(), "count": count, "error": None}
        return count

    async def _recovery_loop(self) -> None:
        """启动时立刻补一次，OneBot 重连时补一次，之后每 RECOVERY_SWEEP_SECONDS 扫一次。

        经常扫的理由：写前日志之后的重活失败时，消息会停在 `state=pending`；
        不主动扫的话它要等到下次重启才被处理 —— 那就不是"绝不静默丢弃"了。
        """
        last_connected = False
        last_sweep: float | None = None
        loop = asyncio.get_running_loop()
        while True:
            try:
                connected = bool(self.hub.connected)
                reconnected = connected and not last_connected
                now = loop.time()
                # 首次（last_sweep is None）必须立刻扫一次：上次崩溃留下的 pending
                # 不能等到 60 秒后才开始补
                if reconnected or last_sweep is None or (now - last_sweep) >= RECOVERY_SWEEP_SECONDS:
                    count = await self.run_recovery()
                    if count:
                        logger.info("崩溃恢复：本轮补处理了 %d 条消息", count)
                    last_sweep = now
                last_connected = connected
                if reconnected:
                    # 重连后顺手看一眼有没有群静默了（断线期间的缺口）
                    await startup_gap_check(self.backend, self.settings)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("崩溃恢复循环异常：%s", exc)
                self.last_recovery = {"at": now_ms(), "count": 0, "error": str(exc)}
            await asyncio.sleep(max(1.0, float(self.settings.recovery_check_interval)))

    # ---------------- 状态 ----------------

    async def _safe(self, coro, default, what: str):
        """后端调用失败时退回默认值：状态页必须永远能返回 200。"""
        try:
            return await coro
        except BackendError as exc:
            logger.warning("状态页读取 %s 失败：%s", what, exc)
            return default
        except Exception as exc:
            logger.warning("状态页读取 %s 异常：%s", what, exc)
            return default

    async def status_payload(self) -> dict:
        """`GET /api/status` 的完整结构（契约第 9 节）。

        盲区计数、群列表、缺口告警全部**现算**：后端只剩存储，
        它不知道"盲区"是什么意思。
        """
        settings = self.settings
        backend = self.backend
        day = local_day()
        now = now_ms()

        health = await backend.health()
        reachable = bool(health.get("reachable"))

        stat: dict = {}
        groups: list[dict] = []
        alerts: list[dict] = []
        unparsed_count = 0
        conflict_count = 0
        low_confidence_count = 0
        digest_sent_count = 0

        if reachable:
            (
                stat,
                groups,
                alerts,
                unparsed_count,
                conflict_count,
                low_confidence_count,
                digest_sent_count,
            ) = await asyncio.gather(
                self._safe(backend.get_stats(day), {}, "stats"),
                self._safe(backend.list_groups(), [], "groups"),
                self._safe(backend.list_gap_alerts(acknowledged=False, limit=20), [], "gap-alerts"),
                self._safe(
                    backend.count_messages(
                        state=["unparsed", "degraded"],
                        since=now - UNPARSED_WINDOW_DAYS * 24 * 3600 * 1000,
                    ),
                    0,
                    "unparsed count",
                ),
                self._safe(backend.count_notifications(conflict=True), 0, "conflict count"),
                self._safe(
                    backend.count_notifications(
                        low_confidence_below=settings.low_confidence_threshold
                    ),
                    0,
                    "low confidence count",
                ),
                self._safe(
                    backend.count_digest_logs(day=day, kind="auto", sent=True), 0, "digest log"
                ),
            )

        return {
            "onebot": self.hub.status(),
            "llm": {
                "extractor": settings.extractor,
                "primary_model": target_from_settings(settings, "primary").label,
                "secondary_model": (
                    target_from_settings(settings, "secondary").label
                    if settings.cross_check_enabled
                    else None
                ),
                "cross_check_enabled": settings.cross_check_enabled,
                "vlm_enabled": settings.vlm_enabled,
            },
            "whitelist": {
                "groups": [
                    {"group_id": gid, "name": name}
                    for gid, name in settings.group_display_names.items()
                ],
                "senders": [
                    {"sender_id": sid, "name": name}
                    for sid, name in settings.sender_whitelist_map.items()
                ],
                "sender_mode": settings.sender_whitelist_mode,
                # 白名单是否配到了"能收到东西"。前端可以据此提示"当前不会处理任何消息"。
                "ready": settings.whitelist_ready,
            },
            "pipeline": {
                "today_ingested": int(stat.get("ingested") or 0),
                "today_extracted": int(stat.get("extracted") or 0),
                "today_unparsed": int(stat.get("unparsed") or 0),
                "today_conflicts": int(stat.get("conflicts") or 0),
                "today_degraded": int(stat.get("degraded") or 0),
                "today_llm_tokens": int(stat.get("llm_tokens") or 0),
                # bot 自己的运行时计数（不是后端的）
                "queue_depth": self.pipeline.depth,
                "processed": self.pipeline.processed,
                "deferred": self.pipeline.dropped,
                # 连原文都没能写进后端的消息条数。>0 就说明有东西真的可能有风险
                "pending_retry": len(self.backend.pending),
                "pending_retry_items": self.backend.pending.snapshot()[-5:],
            },
            "blindspots": {
                "unparsed_count": unparsed_count,
                "conflict_count": conflict_count,
                "low_confidence_count": low_confidence_count,
                "degraded_today": int(stat.get("degraded") or 0) > 0,
                "window_days": UNPARSED_WINDOW_DAYS,
            },
            "groups": self._merge_groups(groups, now),
            "gap_alerts": alerts,
            "backend": health,
            "recovery": self.last_recovery,
            "digest": {
                "enabled": settings.digest_enabled,
                "time": settings.digest_time,
                "target_qq": settings.digest_target_qq,
                "sent_today": digest_sent_count > 0,
            },
            "day": day,
            "server_time": now,
        }

    def _merge_groups(self, backend_groups: list[dict], now: int) -> list[dict]:
        """后端 group_state 是真相来源，bot 的 GroupRegistry 只用来补群名。

        群名缓存可以放内存（丢了会重新查，后端 group_state 里也有），
        但**不能当真相来源** —— 它的 last_msg_ts 只覆盖 bot 这次启动之后见过的消息。
        """
        settings = self.settings
        display = settings.group_display_names
        rows: dict[str, dict] = {}

        for g in backend_groups:
            gid = str(g.get("group_id") or "")
            if not gid:
                continue
            rows[gid] = {
                "group_id": gid,
                "group_name": g.get("group_name") or display.get(gid),
                "in_whitelist": settings.in_group_whitelist(gid),
                "last_msg_ts": g.get("last_msg_ts"),
                "msg_count_today": g.get("msg_count_today"),
            }

        for g in self.normalizer.groups.snapshot():
            gid = str(g.get("group_id") or "")
            if not gid:
                continue
            row = rows.setdefault(
                gid,
                {
                    "group_id": gid,
                    "group_name": None,
                    "in_whitelist": settings.in_group_whitelist(gid),
                    "last_msg_ts": None,
                    "msg_count_today": None,
                },
            )
            row["group_name"] = row.get("group_name") or g.get("group_name")
            row["last_msg_ts"] = row.get("last_msg_ts") or g.get("last_msg_ts")

        # 白名单里配了但一直没消息的群也要出现在列表里（否则"配错了群号"永远看不见）
        for gid in settings.group_whitelist_map:
            rows.setdefault(
                gid,
                {
                    "group_id": gid,
                    "group_name": display.get(gid),
                    "in_whitelist": True,
                    "last_msg_ts": None,
                    "msg_count_today": None,
                },
            )

        out = []
        for row in rows.values():
            last = row.get("last_msg_ts")
            row["last_msg_at"] = last
            try:
                row["silent_hours"] = round((now - int(last)) / 3600000, 2) if last else None
            except (TypeError, ValueError):
                row["silent_hours"] = None
            out.append(row)
        # 白名单内的排前面，然后按最近说话时间倒序
        out.sort(key=lambda r: (not r["in_whitelist"], -(r.get("last_msg_ts") or 0)))
        return out


_runtime: BotRuntime | None = None


def get_runtime() -> BotRuntime:
    global _runtime
    if _runtime is None:
        _runtime = BotRuntime()
    return _runtime


def set_runtime(runtime: BotRuntime | None) -> None:
    """自检工具用：把构造好的 runtime 挂上去（避免再建一个连真 NapCat 的 hub）。"""
    global _runtime
    _runtime = runtime


# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = get_runtime()
    await runtime.start()
    try:
        yield
    finally:
        await runtime.stop()


app = FastAPI(title="Xcollector bot", version=__version__, lifespan=lifespan)

api = APIRouter(prefix="/api")


# ---------------------------------------------------------------------------
# 认证：实现见 app/auth.py
#
# 两个范围：管理令牌（BOT_API_TOKEN / API_TOKEN）什么都能调；网页令牌
# （WEB_API_TOKEN）只能看状态和预览摘要 —— 发消息类的接口一律 403。
# 理由：网页令牌要给登录页，而"任何人拿到它就能以你的身份发 QQ 消息"
# 是比"能改数据库"更直接的后果。
# ---------------------------------------------------------------------------

AuthDep = Annotated[Scope, Depends(require_token)]
WriteDep = Annotated[Scope, Depends(require_write)]


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------


class SendPrivateBody(BaseModel):
    user_id: str
    message: str

    @field_validator("user_id", mode="before")
    @classmethod
    def _coerce_id(cls, value: object) -> str:
        # 调用方可能把 QQ 号当数字发过来（JSON 里 10001 和 "10001" 都是合理的），
        # 这里统一成字符串，避免 pydantic v2 严格模式下直接 422。
        return "" if value is None else str(value)

    @field_validator("message", mode="before")
    @classmethod
    def _coerce_message(cls, value: object) -> str:
        return "" if value is None else str(value)


class SendGroupBody(BaseModel):
    group_id: str
    message: str

    @field_validator("group_id", mode="before")
    @classmethod
    def _coerce_id(cls, value: object) -> str:
        return "" if value is None else str(value)

    @field_validator("message", mode="before")
    @classmethod
    def _coerce_message(cls, value: object) -> str:
        return "" if value is None else str(value)


class DigestSendBody(BaseModel):
    dry_run: bool = True


# ---------------------------------------------------------------------------
# 发送
# ---------------------------------------------------------------------------


async def _do_send(kind: str, target: str, message: str) -> dict:
    """统一的发送实现。**任何失败都返回 ok=false，不抛 500。**

    调用方要能区分两件事：
      - HTTP 4xx/5xx：bot 自己出问题了（token 不对、请求体不合法）；
      - {"ok": false, "error": ...}：bot 收到了请求，但发不出去
        （OneBot 没连上、QQ 那边拒绝）。
    混成一个 500 的话，运维完全没法判断该去看 bot 还是看 NapCat。
    """
    text = truncate(message or "", MESSAGE_MAX_CHARS, TRUNCATE_SUFFIX)
    if not target:
        return {"ok": False, "error": f"{kind}_id 不能为空"}
    if not text:
        return {"ok": False, "error": "message 不能为空"}

    hub = get_runtime().hub
    try:
        if kind == "private":
            resp = await hub.send_private_msg(target, text)
        else:
            resp = await hub.send_group_msg(target, text)
    except OneBotNotConnected as exc:
        return {"ok": False, "error": f"OneBot 未连接：{exc}"}
    except asyncio.TimeoutError:
        return {"ok": False, "error": "发送超时：NapCat 没有返回这次调用（echo 未匹配）"}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    problem = interpret_send_result(resp)
    if problem:
        return {"ok": False, "error": f"NapCat 返回失败：{problem}"}
    return {"ok": True, "error": None}


@api.post("/send/private")
async def send_private(body: SendPrivateBody, _: WriteDep) -> dict:
    return await _do_send("private", body.user_id, body.message)


@api.post("/send/group")
async def send_group(body: SendGroupBody, _: WriteDep) -> dict:
    return await _do_send("group", body.group_id, body.message)


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------


@api.get("/status")
async def status(_: AuthDep) -> dict:
    """给前端「系统状态」页用（契约第 9 节）。

    后端不可达时也**必须返回 200**：页面要能显示"后端挂了"这件事本身，
    所以 status_payload() 里每个后端调用都有兜底默认值。
    """
    return await get_runtime().status_payload()


# ---------------------------------------------------------------------------
# digest（契约第 8 节：后端不再提供，由 bot 组装 + 自己发）
# ---------------------------------------------------------------------------


@api.get("/digest/preview")
async def digest_preview(_: AuthDep) -> dict:
    runtime = get_runtime()
    text = await build_digest(runtime.backend, runtime.settings)
    return {"text": text}


@api.post("/digest/send")
async def digest_send(body: DigestSendBody, _: WriteDep) -> dict:
    runtime = get_runtime()
    return await send_digest(
        dry_run=bool(body.dry_run),
        kind="manual",
        backend=runtime.backend,
        sender=runtime.hub,
    )


app.include_router(api)


@app.get("/")
async def root() -> dict:
    return {
        "name": "Xcollector bot",
        "version": __version__,
        "docs": "/docs",
        "status": "/api/status",
        "onebot": get_runtime().hub.status(),
    }


if _settings.onebot_mode == "server":
    # 注意：真正的反向 WS 由 ReverseWsServer 在 ONEBOT_LISTEN_PORT 上提供（lifespan 里启动）。
    # 这里不再往主应用上挂 WS 路由，避免"到底连的是哪个端口"变成排查时的迷雾。
    logger.debug("server 模式：反向 WS 将监听 %s", _settings.onebot_server_target)


def main() -> None:
    import uvicorn

    s = get_settings()
    uvicorn.run(
        "app.main:app",
        host=s.bot_listen_host,
        port=s.bot_listen_port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
