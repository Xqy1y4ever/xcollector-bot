"""FastAPI 应用入口 + 生命周期。

    uvicorn app.main:app --port 8082
或：
    python -m app.main

这个进程同时干两件事，但它们是**两个不同的端口**，别搞混：
  - BOT_LISTEN_PORT（默认 8082）：本服务的 HTTP API，给 backend 调；
  - ONEBOT_LISTEN_PORT（默认 8081，仅 server 模式）：给 NapCat 的反向 WS 用。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, nullcontext, suppress
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, WebSocket
from pydantic import BaseModel, field_validator

from . import __version__
from .backend_client import BackendClient
from .commands import CommandRouter, log_forward
from .config import Settings, get_settings
from .logging_setup import setup_logging
from .normalize import MessageNormalizer, NormalizedMessage, message_kind
from .onebot import OneBotHub, OneBotNotConnected
from .utils import truncate

setup_logging()
logger = logging.getLogger("xcollector.bot")
_settings = get_settings()

# /api/send/* 的长度上限。超长消息 NapCat 会直接报错，
# 与其让 backend 去猜为什么发不出去，不如截断后如实告知（后缀就是告知）。
MESSAGE_MAX_CHARS = 1500
TRUNCATE_SUFFIX = "…（已截断）"

# 攒批参数。200ms 是刻意选的：
# 群通知经常是"连发 3~5 条"，200ms 能把它们合成一个请求；
# 同时 200ms 短到用户感觉不出延迟，单条消息也不会为了攒批拖几秒。
BATCH_WINDOW_SECONDS = 0.2
BATCH_MAX_SIZE = 20
# 队列上限：backend 长时间挂掉时不能让消息把内存撑爆
QUEUE_MAX_SIZE = 2000


# ---------------------------------------------------------------------------
# 转发：攒批 + 重试（重试逻辑在 BackendClient 里）
# ---------------------------------------------------------------------------


class MessageForwarder:
    """把归一化后的消息攒成小批送给 backend。

    为什么要有队列而不是直接 await：
      OneBot 的接收循环必须永远是"收下一条"的状态。任何在这里的 await
      都可能把接收卡住，而 backend 挂了是常态（重启、升级、抽风）。
      队列把两边的时间尺度解耦开：接收永远不等待，发送慢就慢在后台。
    """

    def __init__(self, backend: BackendClient, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.backend = backend
        self._queue: asyncio.Queue[NormalizedMessage] = asyncio.Queue(QUEUE_MAX_SIZE)
        self._task: asyncio.Task | None = None
        self._dropped = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self._final_flush()

    async def submit(self, msg: NormalizedMessage) -> None:
        """收下一条消息。永不阻塞、永不抛异常。"""
        try:
            self._queue.put_nowait(msg)
            return
        except asyncio.QueueFull:
            pass

        # 队列满 = backend 已经挂了很久。丢最旧的而不是拒收最新的：
        # 旧通知大概率已经被后来的消息覆盖，新消息更接近用户现在关心的事。
        with suppress(asyncio.QueueEmpty):
            self._queue.get_nowait()
        self._dropped += 1
        if self._dropped == 1 or self._dropped % 100 == 0:
            logger.error("转发队列已满，丢弃最旧的消息（累计已丢 %d 条）", self._dropped)
        with suppress(asyncio.QueueFull):
            self._queue.put_nowait(msg)

    # ---------------- 内部 ----------------

    async def _flush_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            first = await self._queue.get()
            batch = [first]
            deadline = loop.time() + BATCH_WINDOW_SECONDS
            while len(batch) < BATCH_MAX_SIZE:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), remaining))
                except asyncio.TimeoutError:
                    break
            await self._send(batch)

    async def _send(self, batch: list[NormalizedMessage]) -> bool:
        ok = await self.backend.ingest_messages([m.to_dict() for m in batch])
        for msg in batch:
            log_forward(
                ok=ok,
                group_id=msg.group_id,
                group_name=msg.group_name,
                sender_name=msg.sender_name,
                message_id=msg.message_id,
                text=msg.text,
            )
        return ok

    async def _final_flush(self) -> None:
        """关停前尽力把队列里剩下的发出去（best effort，超时就放弃）。"""
        batch: list[NormalizedMessage] = []
        while not self._queue.empty() and len(batch) < BATCH_MAX_SIZE * 5:
            with suppress(asyncio.QueueEmpty):
                batch.append(self._queue.get_nowait())
        if not batch:
            return
        logger.info("关停前转发剩余的 %d 条消息", len(batch))
        try:
            await asyncio.wait_for(self._send(batch), timeout=3.0)
        except Exception as exc:
            logger.warning("关停前转发失败（已放弃）：%s", exc)


# ---------------------------------------------------------------------------
# 反向 WS 专用服务（server 模式）
# ---------------------------------------------------------------------------


def build_onebot_ws_app() -> FastAPI:
    """反向 WS 的专用小应用：只有一条路由，别的什么都不暴露。

    为什么要单独起一个 server 而不是挂在主应用上：
    `ONEBOT_LISTEN_PORT`(8081) 和 `BOT_LISTEN_PORT`(8082) 是**两个不同的端口**，
    一个给 NapCat 连、一个给 backend 调。uvicorn 一个实例只监听一个端口，
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
    """把 hub / backend / 归一化 / 指令串起来。

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
        self.forwarder = MessageForwarder(self.backend, self.settings)
        self.ws_server = ReverseWsServer(self.settings)

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
        logger.info("后端地址：%s", settings.backend_base)

        if not settings.bot_api_token:
            # 这条 WARNING 是刻意留的：/api/send/* 能冒充机器人发言，
            # 忘了配 token 就等于把它暴露给任何能访问这个端口的人。
            logger.warning(
                "BOT_API_TOKEN 为空：/api/* 不校验认证，仅限本地开发使用"
            )
        if not settings.command_whitelist_map:
            logger.warning("COMMAND_WHITELIST 为空：当前没有任何 QQ 号能发指令")

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
        await self.forwarder.start()

    async def stop(self) -> None:
        await self.forwarder.stop()
        if self.settings.onebot_mode == "server":
            await self.ws_server.stop()
        await self.hub.stop()
        await self.backend.close()
        logger.info("已关闭")

    async def handle_event(self, event: dict) -> None:
        """OneBot 事件总入口。"""
        kind = message_kind(event)
        if kind == "group":
            msg = await self.normalizer.normalize(event, self.hub)
            if msg is None:
                return
            await self.forwarder.submit(msg)
        elif kind == "private":
            await self.router.handle_private_event(event)
        else:
            # discuss（讨论组）等暂不支持。只记 DEBUG，避免刷日志。
            logger.debug("忽略事件类型 message_type=%s", event.get("message_type"))

    def status_payload(self) -> dict:
        payload = self.hub.status()
        # groups 是"消息层"的知识（谁发过言），由归一化层维护
        payload["groups"] = self.normalizer.groups.snapshot()
        return payload


_runtime: BotRuntime | None = None


def get_runtime() -> BotRuntime:
    global _runtime
    if _runtime is None:
        _runtime = BotRuntime()
    return _runtime


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
# 认证
# ---------------------------------------------------------------------------


async def require_token(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """校验后端带来的 Bearer token。

    为什么用普通依赖而不是中间件：这样每个路由的签名里都能看到"这里要认证"，
    将来加一个不需要认证的探活接口（比如 /healthz）也不会被误伤。
    """
    token = get_settings().bot_api_token
    if not token:
        return  # 空 token = 本地开发模式，启动时已经打过 WARNING
    if authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="无效的 BOT_API_TOKEN")


AuthDep = Annotated[None, Depends(require_token)]


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------


class SendPrivateBody(BaseModel):
    user_id: str
    message: str

    @field_validator("user_id", mode="before")
    @classmethod
    def _coerce_id(cls, value: object) -> str:
        # 后端可能把 QQ 号当数字发过来（JSON 里 10001 和 "10001" 都是合理的），
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


# ---------------------------------------------------------------------------
# 发送
# ---------------------------------------------------------------------------


def _check_send_result(resp: object) -> str | None:
    """检查 OneBot 的 API 响应，失败时返回错误描述。

    hub.call_api 拿到响应就算"调用成功"，但 NapCat 可能返回
    status=failed / retcode!=0（比如"该群不存在"、"机器人被禁言"）。
    这两种失败必须区分：前者是 bot 的问题，后者是 QQ 那边的问题。
    """
    if not isinstance(resp, dict):
        return None
    status = resp.get("status")
    retcode = resp.get("retcode")
    if status in (None, "ok") and retcode in (None, 0):
        return None
    detail = resp.get("message") or resp.get("wording") or f"retcode={retcode}"
    return str(detail)


async def _do_send(kind: str, target: str, message: str) -> dict:
    """统一的发送实现。**任何失败都返回 ok=false，不抛 500。**

    后端要能区分两件事：
      - HTTP 4xx/5xx：bot 自己出问题了（token 不对、请求体不合法）；
      - {"ok": false, "error": ...}：bot 收到了请求，但发不出去
        （OneBot 没连上、QQ 那边拒绝）。
    混成一个 500 的话，backend 的 digest 就只能记一句"调用失败"，
    运维完全没法判断该去看 bot 还是看 NapCat。
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

    problem = _check_send_result(resp)
    if problem:
        return {"ok": False, "error": f"NapCat 返回失败：{problem}"}
    return {"ok": True, "error": None}


@api.post("/send/private")
async def send_private(body: SendPrivateBody, _: AuthDep) -> dict:
    return await _do_send("private", body.user_id, body.message)


@api.post("/send/group")
async def send_group(body: SendGroupBody, _: AuthDep) -> dict:
    return await _do_send("group", body.group_id, body.message)


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------


@api.get("/status")
async def status(_: AuthDep) -> dict:
    """给 backend 看"bot 到底活着没有、看得见哪些群"。

    last_event_at 用的是**事件**时间（含心跳），不是消息时间 ——
    它回答的是"WS 还通吗"，而不是"群里有人在说话吗"。
    """
    return get_runtime().status_payload()


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
