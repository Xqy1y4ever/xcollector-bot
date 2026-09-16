"""调用 backend 的 HTTP 客户端。

两件事混在一起会很乱，所以这里分得很清楚：

  1. **单向转发**（ingest）：消息只往一个方向流，bot 不等 backend 的结果。
     失败就重试，仍失败就丢弃 + 记 ERROR —— 绝不让 backend 的抖动
     影响到 OneBot 的接收循环。
  2. **交互式调用**（/add、/list、/done、/del）：用户在群里/私聊里等着回复，
     所以**失败要快**（只试一次），并把错误翻译成一句人话给用户。

为什么要这么分：这两类请求对"延迟 vs 成功率"的取舍完全相反。
转发可以为了不丢消息等 7 秒；指令让用户干等 7 秒只会显得机器人死了。
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from .config import Settings, get_settings

logger = logging.getLogger(__name__)

# 指数退避的起点：1s / 2s / 4s ...
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0


class BackendError(RuntimeError):
    """调 backend 失败的基类。"""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class BackendUnavailable(BackendError):
    """连不上 / 超时 / 5xx —— 属于"后端暂时不可用"。"""


class BackendRejected(BackendError):
    """4xx —— 后端明确拒绝了这次请求（参数不对、对象不存在等）。

    和 Unavailable 分开，是因为对用户的措辞完全不同：
    「稍后再试」和「这个操作不被接受」是两码事。
    """


def _detail(resp: httpx.Response) -> str:
    """尽量从后端响应里掏出人话（FastAPI 的 detail 字段）。"""
    try:
        body = resp.json()
    except Exception:
        return (resp.text or "").strip()[:200]
    if isinstance(body, dict):
        detail = body.get("detail") or body.get("error")
        if detail:
            return str(detail)[:200]
    return str(body)[:200]


class BackendClient:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        headers: dict[str, str] = {}
        if self.settings.ingest_api_token:
            headers["Authorization"] = f"Bearer {self.settings.ingest_api_token}"
        self._client = httpx.AsyncClient(
            base_url=self.settings.backend_base,
            timeout=self.settings.backend_timeout,
            headers=headers,
        )

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # 底层请求 + 重试
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json: dict | None = None,
        purpose: str = "",
        retries: int | None = None,
        retry_on_4xx: bool = False,
    ) -> httpx.Response:
        """发一次请求，失败按指数退避重试。

        retries = 重试次数（不含首次尝试）。退避序列 1s / 2s / 4s ...
        """
        total = self.settings.backend_max_retries if retries is None else retries
        attempts = max(1, int(total) + 1)
        delay = RETRY_BASE_DELAY
        last_error: BackendError | None = None

        for attempt in range(1, attempts + 1):
            try:
                resp = await self._client.request(method, url, json=json)
            except Exception as exc:
                # httpx 的超时/连接错误都是 Exception 子类；CancelledError 不是，
                # 所以取消操作不会被这里吞掉。
                last_error = BackendUnavailable(f"{type(exc).__name__}: {exc}")
            else:
                if 200 <= resp.status_code < 300:
                    return resp

                detail = _detail(resp)
                if resp.status_code < 500 and not retry_on_4xx:
                    # 重试改变不了 4xx 的结果，而且可能重复副作用（比如重复建任务）
                    raise BackendRejected(
                        f"HTTP {resp.status_code}: {detail}", resp.status_code
                    )
                last_error = BackendUnavailable(f"HTTP {resp.status_code}: {detail}")

            if attempt < attempts:
                logger.warning(
                    "调用后端失败（%s，第 %d/%d 次）：%s，%.0fs 后重试",
                    purpose or url,
                    attempt,
                    attempts,
                    last_error,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_DELAY)

        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------------
    # 单向：消息转发
    # ------------------------------------------------------------------

    async def ingest_messages(self, messages: list[dict]) -> bool:
        """把一批归一化消息推给 backend。

        返回是否成功。**不抛异常** —— 调用方是接收链路上的后台任务，
        让它为了后端故障去处理异常没有意义，失败已经在这里记清楚了。
        """
        if not messages:
            return True
        try:
            await self._request(
                "POST",
                "/api/ingest/messages",
                json={"messages": messages},
                purpose=f"ingest {len(messages)} 条",
                # 转发链路上 4xx 也重试：多半是后端刚重启/路由还没挂上，
                # 等一两秒再试往往就成功了。真正持续的 4xx 会在重试耗尽后记 ERROR。
                retry_on_4xx=True,
            )
        except BackendError as exc:
            logger.error(
                "转发失败，丢弃 %d 条消息（已重试 %d 次）：%s",
                len(messages),
                self.settings.backend_max_retries,
                exc,
            )
            return False
        return True

    # ------------------------------------------------------------------
    # 交互：指令用
    # ------------------------------------------------------------------

    async def create_manual_task(
        self,
        *,
        text: str,
        sender_id: str,
        sender_name: str,
        auto_commit: bool = True,
        force_commit: bool = False,
    ) -> dict:
        """把用户手写的一句话交给后端解析成任务。"""
        payload: dict = {
            "text": text,
            "sender_id": str(sender_id),
            "sender_name": sender_name,
            "auto_commit": auto_commit,
        }
        if force_commit:
            payload["force_commit"] = True
        resp = await self._request(
            "POST",
            "/api/tasks/manual",
            json=payload,
            purpose="tasks/manual",
            retries=0,  # 用户在线等，不做退避重试
        )
        return resp.json()

    async def list_notifications(self, *, status: str = "active", limit: int = 50) -> list[dict]:
        """GET /api/notifications?status=active&limit=N → 通知列表。

        查询参数拼在 URL 上（而不是走 httpx 的 params），是为了让所有请求
        都从同一条 _request 路径出去 —— 重试和错误翻译只留一份实现。
        """
        resp = await self._request(
            "GET",
            f"/api/notifications?status={status}&limit={int(limit)}",
            purpose="notifications",
            retries=0,  # 用户在线等，不做退避重试
        )
        body = resp.json()
        if isinstance(body, dict):
            return body.get("notifications") or []
        return body or []

    async def correct_notification(
        self, notif_id: str, *, field: str, value: object, user_id: str
    ) -> dict:
        resp = await self._request(
            "POST",
            f"/api/notifications/{notif_id}/corrections",
            json={"field": field, "value": value, "user_id": user_id},
            purpose="corrections",
            retries=0,
        )
        return resp.json()
