"""OneBot 11 连接管理。

支持两种模式（NapCat 那边要跟着配）：
  - client：本服务主动连 NapCat 的 WebSocket 服务地址
  - server：本服务监听一个 WS 端口，NapCat 用「反向 WebSocket」连过来

两种模式共用同一个连接对象，所以下游（归一化 / 指令 / HTTP API）
不需要关心用的是哪种 —— 这是把 NapCat 的部署形态和业务代码解耦的关键。

本文件从 backend 的 `app/onebot/hub.py` 移植过来，改造点：
  1. 配置来源换成 bot 自己的 `app.config`（不再有群白名单这类抽取侧配置）；
  2. 补上 send_group_msg —— digest 现在由 backend 反向调 bot 来发；
  3. server 模式的连接断开不再往上抛异常（Starlette 会把 WebSocketDisconnect
     记成 500 级别的日志噪音）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import suppress
from typing import Any, Awaitable, Callable

from ..config import Settings, get_settings
from ..utils import new_id, now_ms

logger = logging.getLogger(__name__)

try:  # websockets >= 13 的新 asyncio 实现
    from websockets.asyncio.client import connect as _ws_connect  # type: ignore
except Exception:  # pragma: no cover - 兼容 websockets 12 的旧路径
    from websockets.client import connect as _ws_connect  # type: ignore

MAX_FRAME = 32 * 1024 * 1024  # 合并转发的消息体可能很大

# 一次连接存活超过这个秒数，才算"真的连上了"。
#
# 为什么按**存活时长**而不是按异常类型区分正常/异常断开：websockets 在干净关闭时
# 也是**抛** ConnectionClosedOK（而不是让 recv() 返回 None），所以"对端重启导致的
# 正常断开"和"连上就被踢"在异常类型上长得一模一样，只有时长能区分。
#
# 区分开的意义：
#   存活够久 → 配置是对的，退避清零、立刻重连；
#   刚连上就被踢 → 多半是配置问题（Token 不一致之类），退避增长，既不刷屏
#                 也不反复锤对端。
STABLE_SECONDS = 5.0


def _retry_delay(consecutive_immediate_failures: int) -> float:
    """连续发生这么多次"连上就被踢"之后，下一次等多久。

    0（上一次连接是稳定的，或还没失败过）→ 1 秒，等价于立刻重连。
    之后依次 1、2、4、8、16、32、60 秒封顶。
    """
    if consecutive_immediate_failures <= 0:
        return 1.0
    return min(2.0 ** (consecutive_immediate_failures - 1), 60.0)


def _close_code(exc: BaseException) -> int | None:
    """从 websockets 的 ConnectionClosed 里取关闭码，取不到返回 None。

    不同大版本把关闭帧放在 `.rcvd` / `.sent`（Close 对象）或直接放 `.code`，
    这里都试一遍。取不到不算错误，只是提示里少一个数字。
    """
    for attr in ("rcvd", "sent"):
        code = getattr(getattr(exc, attr, None), "code", None)
        if isinstance(code, int):
            return code
    code = getattr(exc, "code", None)
    return code if isinstance(code, int) else None


def _explain_disconnect(exc: BaseException, lasted: float) -> str:
    """把断开原因翻译成"下一步该查什么"。

    只打一句 `ConnectionClosedOK: received 1005` 是没用的 —— 1005 的意思是
    对端根本没给状态码，光看它推不出任何结论。所以这里直接给出最可能的原因
    和该去哪里确认。
    """
    if lasted >= STABLE_SECONDS:
        return f"连接断开（已稳定运行 {lasted:.0f}s，多为对端重启或网络抖动），正在重连"

    code = _close_code(exc)
    shown = "未给出" if code is None else str(code)
    return (
        f"刚连上 {lasted:.1f}s 就被对端关闭（关闭码 {shown}）。"
        "最常见的原因是 NapCat 的 WebSocket 服务器配了 Token，而 ONEBOT_ACCESS_TOKEN "
        "没填或不一致 —— 照着 NapCat 里的值填，或改用 "
        "ws://…:3001/?access_token=<token> 这种带在 URL 上的形式。"
        "其次确认那个端口上确实是 NapCat 的 WebSocket 服务器。"
        "确切原因以 NapCat 自己的日志为准。"
    )


class OneBotNotConnected(RuntimeError):
    """没有任何 OneBot 连接时调用 API 会抛这个。

    HTTP 层捕获它并返回 {"ok": false, "error": ...}，
    这样调用方能区分「调 bot 失败」和「bot 调 NapCat 发送失败」。
    """


def interpret_send_result(resp: object) -> str | None:
    """检查 OneBot 的 API 响应，失败时返回错误描述。

    hub.call_api 拿到响应就算"调用成功"，但 NapCat 可能返回
    status=failed / retcode!=0（比如"该群不存在"、"机器人被禁言"）。
    这两种失败必须区分：前者是 bot 的问题，后者是 QQ 那边的问题。

    放在 hub 里而不是各自实现一份：`/api/send/*` 和 digest 都要用它，
    两处对"什么叫发送失败"的判断必须完全一致。
    """
    if not isinstance(resp, dict):
        return None
    status = resp.get("status")
    retcode = resp.get("retcode")
    if status in (None, "ok") and retcode in (None, 0):
        return None
    detail = resp.get("message") or resp.get("wording") or f"retcode={retcode}"
    return str(detail)


class _Conn:
    """把 websockets 与 Starlette WebSocket 归一化成 send/recv。

    保留这个抽象是刻意的：正向连（websockets 的 ClientConnection）
    和反向接入（Starlette 的 WebSocket）API 名字不一样（send/recv vs
    send_text/receive_text），如果让业务代码判断用的是哪种，
    每加一个动作就要写两遍分支。现在只需要在这一个类里分叉。
    """

    def __init__(self, ws: Any, kind: str):
        self.ws = ws
        self.kind = kind  # 'client' | 'server'

    async def send_text(self, text: str) -> None:
        if self.kind == "server":
            await self.ws.send_text(text)
        else:
            await self.ws.send(text)

    async def recv_text(self) -> str:
        if self.kind == "server":
            return await self.ws.receive_text()
        return await self.ws.recv()

    async def close(self) -> None:
        try:
            await self.ws.close()
        except Exception:
            pass


class OneBotHub:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._conn: _Conn | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self._on_event: Callable[[dict], Awaitable[None]] | None = None
        self._recv_task: asyncio.Task | None = None
        self._client_task: asyncio.Task | None = None
        self._stopping = False
        self.connected = False
        self.last_event_at: int | None = None
        self.reconnect_count = 0
        self.last_error: str | None = None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def set_event_handler(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        self._on_event = handler

    async def start(self) -> None:
        # server 模式什么都不用做：端口由 FastAPI 的 websocket 路由监听，
        # 连接进来时才走 attach_server_ws。
        if self.settings.onebot_mode == "client":
            self._stopping = False
            self._client_task = asyncio.create_task(self._client_loop())

    async def stop(self) -> None:
        self._stopping = True
        tasks = [t for t in (self._client_task, self._recv_task) if t is not None]
        for task in tasks:
            task.cancel()
        # 要 await 一下被取消的任务：只 cancel 不等待的话，关停后事件循环里
        # 还挂着没收尾的任务，测试里会看到"残留任务"，正常关闭也会多打一堆
        # "Task was destroyed but it is pending"。
        for task in tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task
        self._client_task = None
        self._recv_task = None
        if self._conn:
            await self._conn.close()

    # ------------------------------------------------------------------
    # client 模式
    # ------------------------------------------------------------------

    async def _client_loop(self) -> None:
        immediate_failures = 0
        while not self._stopping:
            started = time.monotonic()
            try:
                await self._connect_once()
                # 接收循环干净结束（正常返回）：等价于一次稳定连接
                immediate_failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                lasted = time.monotonic() - started
                # 按"这次连接活了多久"归零或累加。必须在算 delay **之前**做，
                # 否则会把之前连不上时累积的 60s 带到"稳定跑了一小时才断开"
                # 的场景里 —— 那种情况应该立刻重连。
                if lasted >= STABLE_SECONDS:
                    immediate_failures = 0
                else:
                    immediate_failures += 1
                delay = _retry_delay(immediate_failures)

                self.last_error = f"{type(exc).__name__}: {exc}"
                # 说明单独一行：它往往很长（要给出"下一步查什么"），
                # 跟在错误后面用逗号接一截会读成一句话，看不清哪个是重点。
                logger.warning(
                    "OneBot 连接失败（%.0fs 后重试）: %s\n    ↳ %s",
                    delay,
                    self.last_error,
                    _explain_disconnect(exc, lasted),
                )
                self.connected = False
                self.reconnect_count += 1
                await asyncio.sleep(delay)

    async def _connect_once(self) -> None:
        url = self.settings.onebot_ws_url
        headers = {}
        if self.settings.onebot_access_token:
            headers["Authorization"] = f"Bearer {self.settings.onebot_access_token}"

        kwargs: dict[str, Any] = {
            "max_size": MAX_FRAME,
            "ping_interval": 20,
            "ping_timeout": 20,
        }
        if headers:
            kwargs["additional_headers"] = headers
        try:
            ws = await _ws_connect(url, **kwargs)
        except TypeError:
            # 旧版 websockets 用的是 extra_headers
            if headers:
                kwargs.pop("additional_headers", None)
                kwargs["extra_headers"] = headers
            ws = await _ws_connect(url, **kwargs)

        logger.info("OneBot 已连接 (client): %s", url)
        self._conn = _Conn(ws, "client")
        self.connected = True
        self.last_error = None
        try:
            await self._run_socket(self._conn)
        finally:
            self.connected = False
            self._conn = None
            await ws.close()

    # ------------------------------------------------------------------
    # server 模式（反向 WS）
    # ------------------------------------------------------------------

    async def attach_server_ws(self, websocket: Any) -> None:
        """由 FastAPI 的 websocket 路由调用。"""
        token = self.settings.onebot_access_token
        if token:
            header = websocket.headers.get("authorization", "")
            if header != f"Bearer {token}":
                logger.warning("反向 WS 鉴权失败，已拒绝连接")
                await websocket.close(code=1008)
                return

        await websocket.accept()
        if self.connected:
            # NapCat 重连时旧连接可能还没被回收，这里只记一笔
            self.reconnect_count += 1
        logger.info("OneBot 已连接 (server 反向 WS)")
        conn = _Conn(websocket, "server")
        self._conn = conn
        self.connected = True
        self.last_error = None
        try:
            await self._run_socket(conn)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # 反向 WS 的对端断开在 Starlette 里是异常而非正常返回；
            # 吞掉它只是为了避免每次 NapCat 重启都刷一段 500 的堆栈。
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.debug("反向 WS 断开: %s", exc)
        finally:
            self.connected = False
            self._conn = None

    # ------------------------------------------------------------------
    # 收发
    # ------------------------------------------------------------------

    async def _run_socket(self, conn: _Conn) -> None:
        while True:
            text = await conn.recv_text()
            if text is None:
                break
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                logger.warning("OneBot 收到非法 JSON: %s", str(text)[:200])
                continue
            await self._dispatch(data)

    async def _dispatch(self, data: dict) -> None:
        # 先处理 API 响应（带 echo）
        echo = data.get("echo")
        if echo is not None:
            fut = self._pending.get(str(echo))
            if fut and not fut.done():
                fut.set_result(data)
            return

        self.last_event_at = now_ms()
        post_type = data.get("post_type")
        if post_type in ("message", "message_sent"):
            if self._on_event is None:
                return
            # 关键：不阻塞接收循环。
            # 合并转发展开要发好几次 API 调用，同步处理会把整条 WS 卡住，
            # 期间心跳和别的消息全都收不到。
            asyncio.create_task(self._safe_handle(data))
        elif post_type == "meta_event":
            # 心跳，仅用于刷新 last_event_at
            pass

    async def _safe_handle(self, event: dict) -> None:
        assert self._on_event is not None
        try:
            await self._on_event(event)
        except Exception as exc:  # 单条消息失败绝不能让接收循环挂掉
            logger.exception("消息处理失败: %s", exc)

    async def call_api(
        self, action: str, params: dict | None = None, timeout: float = 15.0
    ) -> dict:
        conn = self._conn
        if conn is None:
            raise OneBotNotConnected("OneBot 未连接")
        echo = new_id()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[echo] = fut
        try:
            await conn.send_text(
                json.dumps({"action": action, "params": params or {}, "echo": echo})
            )
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(echo, None)

    # ------------------------------------------------------------------
    # 常用动作
    # ------------------------------------------------------------------

    async def send_private_msg(self, user_id: str, message: str) -> dict:
        return await self.call_api(
            "send_private_msg",
            {
                "user_id": int(user_id) if str(user_id).isdigit() else user_id,
                "message": message,
            },
            timeout=20,
        )

    async def send_group_msg(self, group_id: str, message: str) -> dict:
        return await self.call_api(
            "send_group_msg",
            {
                "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                "message": message,
            },
            timeout=20,
        )

    async def get_forward_msg(self, forward_id: str) -> list[dict]:
        resp = await self.call_api("get_forward_msg", {"id": forward_id})
        data = resp.get("data") or {}
        return data.get("messages") or []

    async def get_group_info(self, group_id: str) -> dict:
        resp = await self.call_api(
            "get_group_info", {"group_id": group_id, "no_cache": False}
        )
        return resp.get("data") or {}

    # ------------------------------------------------------------------
    # 状态
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """给 /api/status 用。

        注意：返回值里没有 groups —— 群列表是「消息层」的知识（谁发过言），
        由 normalize.GroupRegistry 维护，在 main.py 里合并进来，
        免得连接层也去操心业务语义。
        """
        target = (
            self.settings.onebot_ws_url
            if self.settings.onebot_mode == "client"
            else self.settings.onebot_server_target
        )
        return {
            "mode": self.settings.onebot_mode,
            "connected": self.connected,
            "target": target,
            "last_event_at": self.last_event_at,
            "reconnect_count": self.reconnect_count,
            "last_error": self.last_error,
        }


_hub: OneBotHub | None = None


def get_hub() -> OneBotHub:
    global _hub
    if _hub is None:
        _hub = OneBotHub()
    return _hub


def reset_hub() -> None:
    global _hub
    _hub = None
