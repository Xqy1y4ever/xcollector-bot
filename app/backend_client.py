"""调用 backend 的 HTTP 客户端（契约见 xcollector-backend/docs/api.md）。

拆分之后后端是**纯数据层**：不做任何业务判断，只会增删查改。所以这个文件
就是 bot 与存储之间的全部接口，也是"哪些知识存在 bot 里"的一份清单。

三类调用，对"延迟 vs 成功率"的取舍完全不同：

  1. **写入**（create/patch message & notification、groups、stats、gap-alerts）：
     走 `_write`，失败重试 `BACKEND_MAX_RETRIES` 次（退避 1s/2s/4s）。
     契约保证所有写接口幂等，所以重试是安全的。
  2. **交互式调用**（指令用）：用户在私聊里等着回复，**失败要快**（retries=0），
     并把错误翻译成一句人话给用户。
  3. **状态页调用**（/api/status、digest）：也不重试 —— 状态页要能**显示
     "后端挂了"这件事本身**，为它等 7 秒是反效果。

重试仍失败怎么办：**绝不静默丢**。两条路，按"原文有没有落库"分：
  - 原文**没落库**（POST /api/messages 就失败了）：进内存待重试队列 +
    一行带 `待重试=是` 的 ERROR，队列长度暴露在 /api/status 上；
  - 原文**已落库**（后续步骤失败）：raw 会停在 `state=pending`，
    由 pipeline.runner.resume_pending() 在启动 / 重连 / 每分钟的扫查里补处理，
    这条路的寿命比进程长，是真正的主力。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import Settings, get_settings
from .utils import now_ms

logger = logging.getLogger(__name__)

# 指数退避的起点：1s / 2s / 4s ...
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0

# 4xx 里"等一会儿可能就好了"的几个状态码。
# 后端重启期间路由可能还没挂上（404/405），限流是 429 —— 这些值得重试。
# 而 400/401/403/409/413/422 重试多少次结果都一样，早失败早报错。
RETRYABLE_STATUS = {404, 405, 408, 425, 429}


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


def _is_missing(exc: BackendError) -> bool:
    """这次失败是不是"对象不存在"（404）。

    404 在写接口里被当成"可重试"（后端刚重启时路由可能还没挂上），所以它
    走到最后是以 BackendUnavailable 的形式抛出来的 —— 这里靠状态码认出来，
    避免调用方去猜异常类型。
    """
    return exc.status_code == 404


# ---------------------------------------------------------------------------
# 待重试队列
# ---------------------------------------------------------------------------


@dataclass
class PendingWrite:
    """一条**连原文都没能写进后端**的消息，暂存在内存里等补写。

    存的是归一化消息本身（不是"半截请求"）：补写时要整条重走一遍，
    因为从头到尾所有写接口都是幂等的。
    """

    kind: str  # 目前只有 "message"
    message: dict
    note: str = ""
    attempts: int = 0
    first_failed_at: int = field(default_factory=now_ms)

    def as_dict(self) -> dict:
        return {
            "kind": self.kind,
            "group_id": self.message.get("group_id"),
            "message_id": self.message.get("message_id"),
            "attempts": self.attempts,
            "first_failed_at": self.first_failed_at,
            "note": self.note,
        }


class PendingWrites:
    """进程内待重试队列（只装"原文都没落库"的消息）。

    为什么不落盘：bot 不允许持有需要跨重启存活的状态。原文没落库是**罕见**的
    故障（后端 4xx / 一直不可达），配套动作是打 ERROR + 在 /api/status 的
    `pipeline.pending_retry` 上暴露计数，而不是让 bot 为了它去维护一个本地库。
    队列满时丢**最旧**的：最新的失败更接近当前故障，补写价值更高。
    """

    def __init__(self, maxlen: int = 200):
        self.maxlen = maxlen
        self._items: list[PendingWrite] = []
        self.dropped = 0

    def add(self, item: PendingWrite) -> PendingWrite:
        self._items.append(item)
        while len(self._items) > self.maxlen:
            self._items.pop(0)
            self.dropped += 1
        return item

    def items(self) -> list[PendingWrite]:
        return list(self._items)

    def remove(self, item: PendingWrite) -> None:
        try:
            self._items.remove(item)
        except ValueError:
            pass

    def clear(self) -> None:
        self._items.clear()

    def __len__(self) -> int:
        return len(self._items)

    def snapshot(self) -> list[dict]:
        return [i.as_dict() for i in self._items]


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------


def _is_loopback(url: str) -> bool:
    """`BACKEND_BASE_URL` 指的是本机吗（127.0.0.1 / localhost / ::1 / 0.0.0.0）。"""
    try:
        host = (httpx.URL(url).host or "").lower()
    except Exception:  # noqa: BLE001 - 解析不了就当不是本机（照常走系统代理）
        return False
    return host in ("127.0.0.1", "localhost", "::1", "0.0.0.0")


class BackendClient:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        headers: dict[str, str] = {}
        if self.settings.api_token:
            headers["Authorization"] = f"Bearer {self.settings.api_token}"
        # **本机后端不走代理**：httpx 默认 `trust_env=True`，而它会读 Windows 注册表里的
        # 系统代理（装过 Clash / V2Ray 之类工具的机器上常留着一条 `127.0.0.1:7890`）。
        # 那个代理没开着的时候，连 `http://127.0.0.1:8000` 都会被发过去、然后连接被拒 ——
        # 症状是"后端明明在本机跑着，bot 却说连不上"。公网后端照旧走系统代理。
        self._client = httpx.AsyncClient(
            base_url=self.settings.backend_base,
            timeout=self.settings.backend_timeout,
            headers=headers,
            trust_env=not _is_loopback(self.settings.backend_base),
        )
        self.pending = PendingWrites(self.settings.pending_write_max)

    # ------------------------------------------------------------------
    # 归属（多用户之后每个按用户的接口都要带）
    # ------------------------------------------------------------------

    @staticmethod
    def _owner(user_id: str) -> list[tuple[str, str]]:
        """把 `user_id` 拼成 query 参数。

        bot 用的是**服务令牌**，后端认不出"这次是在替谁办事"，所以每个按用户的
        接口都必须显式带上归属，否则后端一律 400。契约里 `user_id` 全是 query
        参数（不是 body 字段），所以这里统一走这条路径。

        空值直接抛：后端会 400，但那样错误发生在一次网络往返之后，
        而且日志里只看得到一个 HTTP 400 —— 在原地炸掉更容易定位。
        """
        owner = str(user_id or "").strip()
        if not owner:
            raise ValueError("按用户的接口必须带 user_id（bot 用服务令牌，后端认不出归属）")
        return [("user_id", owner)]

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
        params: Any = None,
        files: Any = None,
        data: Any = None,
        purpose: str = "",
        retries: int = 0,
        retry_on_4xx: bool = False,
    ) -> httpx.Response:
        """发一次请求，失败按指数退避重试。

        retries = 重试次数（不含首次尝试）。默认 0：读接口和指令路径都不重试。
        写接口请走 `_write`（它会填上 settings.backend_max_retries）。
        """
        attempts = max(1, int(retries) + 1)
        delay = RETRY_BASE_DELAY
        last_error: BackendError | None = None

        for attempt in range(1, attempts + 1):
            try:
                resp = await self._client.request(
                    method, url, json=json, params=params, files=files, data=data
                )
            except Exception as exc:
                # httpx 的超时/连接错误都是 Exception 子类；CancelledError 不是，
                # 所以取消操作不会被这里吞掉。
                last_error = BackendUnavailable(f"{type(exc).__name__}: {exc}")
            else:
                if 200 <= resp.status_code < 300:
                    return resp

                detail = _detail(resp)
                transient = resp.status_code >= 500 or resp.status_code in RETRYABLE_STATUS
                if not transient and not retry_on_4xx:
                    # 重试改变不了这个 4xx 的结果，而且可能重复副作用
                    raise BackendRejected(
                        f"HTTP {resp.status_code}: {detail}", resp.status_code
                    )
                # 把状态码带上：重试耗尽后调用方仍然需要区分"404=没有这个对象"
                # 和"真的连不上"（见 get_state / delete_state）
                last_error = BackendUnavailable(
                    f"HTTP {resp.status_code}: {detail}", resp.status_code
                )

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

    async def _write(
        self,
        method: str,
        url: str,
        *,
        json: dict | None = None,
        params: Any = None,
        purpose: str = "",
        retries: int | None = None,
    ) -> httpx.Response:
        """写接口：默认按 BACKEND_MAX_RETRIES 重试（1s/2s/4s）。

        `params` 用来带 `user_id`（契约里归属一律是 query 参数，不在 body 里）。
        """
        total = self.settings.backend_max_retries if retries is None else retries
        return await self._request(
            method, url, json=json, params=params, purpose=purpose, retries=total
        )

    async def _json(self, method: str, url: str, **kwargs: Any) -> Any:
        resp = await self._request(method, url, **kwargs)
        if not resp.content:
            return {}
        try:
            return resp.json()
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # 待重试
    # ------------------------------------------------------------------

    def mark_for_retry(
        self,
        *,
        message: dict,
        note: str = "",
    ) -> PendingWrite:
        """把一条**连原文都没能写进后端**的消息放回内存队列，并留一行可 grep 的 ERROR。

        只用于这种最坏情况（后端 4xx / 一直不可达）：消息在别处没有任何副本，
        丢了就真丢了。已经落库、只是没处理完的消息**不走这里** —— 它们停在后端的
        `state=pending` 上，由 pipeline.runner.resume_pending() 负责补处理，
        那条路能扛住 bot 重启，比内存队列可靠。

        调用方：pipeline/runner.py —— 只有它知道"这条消息已经走到哪一步了"。
        """
        item = self.pending.add(PendingWrite(kind="message", message=message, note=note))
        logger.error(
            "原文写入后端彻底失败，已标记待重试=是 | 群=%s(%s) | msg_id=%s | 原因=%s | 队列=%d",
            message.get("group_name"),
            message.get("group_id"),
            message.get("message_id"),
            note,
            len(self.pending),
        )
        return item

    # ------------------------------------------------------------------
    # 消息 raw_message
    # ------------------------------------------------------------------

    async def create_message(self, payload: dict, *, retries: int | None = None) -> dict:
        """POST /api/messages（按 (group_id, message_id) 幂等）→ {id, is_new}。"""
        resp = await self._write(
            "POST", "/api/messages", json=payload, purpose="messages", retries=retries
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def get_message(self, message_id: str) -> dict:
        body = await self._json("GET", f"/api/messages/{message_id}", purpose="messages")
        return body if isinstance(body, dict) else {}

    async def patch_message(self, message_id: str, payload: dict, *, retries: int | None = None) -> dict:
        """PATCH /api/messages/{id} —— 只允许改 state 三个字段（契约保证）。"""
        resp = await self._write(
            "PATCH",
            f"/api/messages/{message_id}",
            json=payload,
            purpose="messages.patch",
            retries=retries,
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_messages(
        self,
        *,
        state: str | list[str] | None = None,
        group_id: str | None = None,
        since: int | None = None,
        limit: int = 100,
    ) -> list[dict]:
        params = self._message_params(state=state, group_id=group_id, since=since, limit=limit)
        body = await self._json("GET", "/api/messages", params=params, purpose="messages")
        if isinstance(body, dict):
            return body.get("messages") or []
        return body or []

    async def count_messages(
        self,
        *,
        state: str | list[str] | None = None,
        group_id: str | None = None,
        since: int | None = None,
    ) -> int:
        """用契约的 `count_only=1` 数条数（盲区计数走这里）。"""
        params = self._message_params(state=state, group_id=group_id, since=since)
        params.append(("count_only", 1))
        body = await self._json("GET", "/api/messages", params=params, purpose="messages.count")
        return int((body or {}).get("count") or 0)

    @staticmethod
    def _message_params(
        *,
        state: str | list[str] | None = None,
        group_id: str | None = None,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, Any]]:
        # 用 list[tuple] 而不是 dict：state 需要重复出现（契约允许"可重复"）
        params: list[tuple[str, Any]] = []
        if state:
            for s in ([state] if isinstance(state, str) else state):
                params.append(("state", s))
        if group_id:
            params.append(("group_id", group_id))
        if since is not None:
            params.append(("since", int(since)))
        if limit is not None:
            params.append(("limit", int(limit)))
        return params

    # ------------------------------------------------------------------
    # 通知 notification
    # ------------------------------------------------------------------

    async def create_notification(
        self, payload: dict, *, user_id: str, retries: int | None = None
    ) -> dict:
        """POST /api/notifications（按 `(user_id, raw_message_id)` 幂等）→ {id, created}。

        `user_id` 是**收件人**。一条原始消息会被扇出成 N 条通知（每个订阅它的
        用户一条），这个参数决定当前这一条是给谁的 —— 漏传就会被后端 400 挡下。

        evidence 为空会被后端 400 拒掉 —— 那条硬约束由后端替 bot 守着。
        """
        resp = await self._write(
            "POST",
            "/api/notifications",
            json=payload,
            params=self._owner(user_id),
            purpose="notifications",
            retries=retries,
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def patch_notification(
        self, notif_id: str, payload: dict, *, user_id: str, retries: int | None = None
    ) -> dict:
        resp = await self._write(
            "PATCH",
            f"/api/notifications/{notif_id}",
            json=payload,
            params=self._owner(user_id),
            purpose="notifications.patch",
            retries=retries,
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_notifications(
        self,
        *,
        user_id: str,
        status: str = "all",
        q: str | None = None,
        since: int | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """GET /api/notifications → 读投影列表（人工修正已生效、status 已推导）。"""
        params: list[tuple[str, Any]] = self._owner(user_id)
        params += [("status", status), ("limit", int(limit))]
        if q:
            params.append(("q", q))
        if since is not None:
            params.append(("since", int(since)))
        body = await self._json("GET", "/api/notifications", params=params, purpose="notifications")
        if isinstance(body, dict):
            return body.get("notifications") or []
        return body or []

    async def get_notification(self, notif_id: str, *, user_id: str) -> dict:
        body = await self._json(
            "GET",
            f"/api/notifications/{notif_id}",
            params=self._owner(user_id),
            purpose="notifications.get",
        )
        return body if isinstance(body, dict) else {}

    async def delete_notification(
        self, notif_id: str, *, user_id: str, retries: int | None = None
    ) -> dict:
        resp = await self._write(
            "DELETE",
            f"/api/notifications/{notif_id}",
            params=self._owner(user_id),
            purpose="notifications.delete",
            retries=retries,
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def correct_notification(
        self, notif_id: str, *, field: str, value: object, actor: str, user_id: str
    ) -> dict:
        """POST /api/notifications/{id}/corrections —— 人工修正（只追加、留痕）。

        两个"用户"字段含义不同，别混：
          - `user_id` 是**租户**（这条修正属于谁的数据）→ query 参数
          - `actor` 是**谁操作的**（QQ 号）→ body 字段，界面上显示"谁改的"
        """
        resp = await self._request(
            "POST",
            f"/api/notifications/{notif_id}/corrections",
            json={"field": field, "value": value, "actor": actor},
            params=self._owner(user_id),
            purpose="corrections",
            retries=0,  # 用户在线等
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def count_notifications(
        self,
        *,
        user_id: str,
        status: str = "all",
        conflict: bool | None = None,
        low_confidence_below: float | None = None,
    ) -> int:
        """数通知条数。

        只按 `status` 过滤时走契约的 `count_only=1`（一次请求一个数字）。
        但契约的 `GET /api/notifications` **没有暴露 conflict / due_confidence
        这两个过滤参数**，所以带这两个条件时只能在 bot 侧拉回来自己数 ——
        盲区计数不能依赖后端没答应的参数（未知参数会被静默忽略，
        那样数出来的是"全部通知"，会静默错得很离谱）。
        """
        if conflict is None and low_confidence_below is None:
            params: list[tuple[str, Any]] = self._owner(user_id)
            params += [("status", status), ("count_only", 1)]
            body = await self._json(
                "GET", "/api/notifications", params=params, purpose="notifications.count"
            )
            return int((body or {}).get("count") or 0)

        rows = await self.list_notifications(user_id=user_id, status=status, limit=2000)
        if conflict is not None:
            rows = [r for r in rows if bool(r.get("conflict")) == bool(conflict)]
        if low_confidence_below is not None:
            rows = [
                r
                for r in rows
                if 0 < float(r.get("due_confidence") or 0.0) < low_confidence_below
            ]
        return len(rows)

    # ------------------------------------------------------------------
    # 附件
    # ------------------------------------------------------------------

    async def upload_attachment(
        self,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        source_url: str | None = None,
    ) -> dict | None:
        """POST /api/attachments（multipart/form-data）→ {id, url, size, content_type}。

        **失败返回 None，不抛异常**：调用方（runner）据此把附件降级成
        "只存 source_url"。为了一张图把整条通知丢掉是本末倒置。

        重试说明：multipart 上传不是幂等的，5xx 之后重试可能在后端留下
        两份字节（只多占点空间，不影响正确性 —— 最终记录的是后一次的 url）。
        """
        form: dict[str, str] = {"filename": filename or "file"}
        if source_url:
            form["source_url"] = source_url
        files = {"file": (filename or "file", content, content_type)}
        try:
            resp = await self._request(
                "POST",
                "/api/attachments",
                files=files,
                data=form,
                purpose=f"attachments({filename})",
                retries=self.settings.backend_max_retries,
            )
        except BackendError as exc:
            logger.warning("附件上传失败 %s（已降级为只存 source_url）：%s", filename, exc)
            return None
        try:
            body = resp.json()
        except Exception:
            logger.warning("附件上传返回了非 JSON：%s", (resp.text or "")[:120])
            return None
        return body if isinstance(body, dict) else None

    # ------------------------------------------------------------------
    # 群状态
    # ------------------------------------------------------------------

    async def upsert_group(
        self, group_id: str, group_name: str | None, last_msg_ts: int
    ) -> dict:
        """POST /api/groups → {group, previous_last_msg_ts}。

        `previous_last_msg_ts` 由后端在**同一次写**里返回，bot 拿它做缺口检测，
        省掉一次"读-判断-写"的竞态窗口。
        """
        resp = await self._write(
            "POST",
            "/api/groups",
            json={
                "group_id": str(group_id),
                "group_name": group_name,
                "last_msg_ts": int(last_msg_ts),
            },
            purpose="groups",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_groups(self) -> list[dict]:
        body = await self._json("GET", "/api/groups", purpose="groups")
        if isinstance(body, dict):
            return body.get("groups") or []
        return body or []

    # ------------------------------------------------------------------
    # 缺口告警
    # ------------------------------------------------------------------

    async def create_gap_alert(
        self,
        *,
        user_id: str,
        group_id: str,
        group_name: str | None,
        from_ts: int,
        to_ts: int,
        reason: str,
    ) -> dict:
        resp = await self._write(
            "POST",
            "/api/gap-alerts",
            json={
                "group_id": str(group_id),
                "group_name": group_name,
                "from_ts": int(from_ts),
                "to_ts": int(to_ts),
                "reason": reason,
            },
            params=self._owner(user_id),
            purpose="gap-alerts",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_gap_alerts(
        self, *, user_id: str, acknowledged: bool | None = None, limit: int = 20
    ) -> list[dict]:
        params: list[tuple[str, Any]] = self._owner(user_id)
        params.append(("limit", int(limit)))
        if acknowledged is not None:
            params.append(("acknowledged", "true" if acknowledged else "false"))
        body = await self._json("GET", "/api/gap-alerts", params=params, purpose="gap-alerts")
        if isinstance(body, dict):
            return body.get("alerts") or []
        return body or []

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------

    async def add_stats(self, day: str | None, fields: dict[str, int], *, user_id: str) -> dict:
        """POST /api/stats —— 后端只做累加，不理解每个字段是什么意思。

        统计是**按用户**的：一条消息被扇给 N 个人，就给这 N 个人各记一次。
        这不是"重复计数" —— 从每个用户的角度看，"为我处理了一条消息"确实
        发生了 N 次里的一次。全站视角的数字在运维层面没人需要，
        而用户视角的数字（"我的源里有多少条没能解析"）才是盲区告警要用的。
        """
        payload: dict = {"fields": {k: int(v) for k, v in fields.items() if v}}
        if day:
            payload["day"] = day
        resp = await self._write(
            "POST", "/api/stats", json=payload, params=self._owner(user_id), purpose="stats"
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def get_stats(self, day: str | None = None, *, user_id: str) -> dict:
        params: list[tuple[str, Any]] = self._owner(user_id)
        if day:
            params.append(("day", day))
        body = await self._json("GET", "/api/stats", params=params, purpose="stats")
        return body if isinstance(body, dict) else {}

    # ------------------------------------------------------------------
    # digest 发送记录（契约第 10 节）
    # ------------------------------------------------------------------

    async def add_digest_log(
        self,
        *,
        user_id: str,
        day: str | None,
        kind: str,
        text: str,
        sent: bool,
        error: str | None = None,
    ) -> dict:
        payload: dict = {"kind": kind, "text": text, "sent": bool(sent), "error": error}
        if day:
            payload["day"] = day
        resp = await self._write(
            "POST",
            "/api/digest-log",
            json=payload,
            params=self._owner(user_id),
            purpose="digest-log",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_digest_logs(
        self,
        *,
        user_id: str,
        day: str | None = None,
        kind: str | None = None,
        sent: bool | None = None,
        limit: int = 50,
    ) -> list[dict]:
        params = self._owner(user_id)
        params += self._digest_log_params(day=day, kind=kind, sent=sent)
        params.append(("limit", int(limit)))
        body = await self._json("GET", "/api/digest-log", params=params, purpose="digest-log")
        if isinstance(body, dict):
            return body.get("logs") or []
        return body or []

    async def count_digest_logs(
        self,
        *,
        user_id: str,
        day: str | None = None,
        kind: str | None = None,
        sent: bool | None = None,
    ) -> int:
        """只问一个数字：「今天 auto 且 sent=true 的有几条」。

        digest 的"今天发过没有"必须问后端 —— bot 不允许持有跨重启存活的状态，
        而重发对收件人是骚扰，比漏发更糟。

        按用户问：A 今天收到过不代表 B 收到过。混在一起会让"今天已经发过"
        把别人的那一份也吞掉 —— 静默漏发，正是最该避免的失败。
        """
        params = self._owner(user_id)
        params += self._digest_log_params(day=day, kind=kind, sent=sent)
        params.append(("count_only", 1))
        body = await self._json(
            "GET", "/api/digest-log", params=params, purpose="digest-log.count"
        )
        return int((body or {}).get("count") or 0)

    @staticmethod
    def _digest_log_params(
        *, day: str | None, kind: str | None, sent: bool | None
    ) -> list[tuple[str, Any]]:
        params: list[tuple[str, Any]] = []
        if day:
            params.append(("day", day))
        if kind:
            params.append(("kind", kind))
        if sent is not None:
            params.append(("sent", "true" if sent else "false"))
        return params

    # ------------------------------------------------------------------
    # bot 的键值暂存（契约第 11 节）
    # ------------------------------------------------------------------

    async def put_state(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        user_id: str,
        ttl_seconds: int | None = None,
    ) -> dict:
        """PUT /api/state/{namespace}/{key} —— 幂等 upsert。

        这块是"带 TTL 的持久化草稿纸"：指令的待确认状态和 /list 的编号映射
        必须跨重启存活，否则用户回 `y` 时那条待确认会凭空消失。

        **也必须按用户分**：两个用户同时 /add 待确认，共用一份就会互相覆盖 ——
        一个人确认掉的可能是另一个人的草稿。
        """
        payload: dict = {"value": value}
        if ttl_seconds is not None:
            payload["ttl_seconds"] = int(ttl_seconds)
        resp = await self._write(
            "PUT",
            f"/api/state/{namespace}/{key}",
            json=payload,
            params=self._owner(user_id),
            purpose=f"state.{namespace}",
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def get_state(self, namespace: str, key: str, *, user_id: str) -> Any | None:
        """GET /api/state/{namespace}/{key}。

        **404 = 没有（或已过期），不是错误** —— 这是这个接口的正常返回值之一，
        所以这里把 404 翻译成 None，让调用方少写一层 try。
        """
        try:
            body = await self._json(
                "GET",
                f"/api/state/{namespace}/{key}",
                params=self._owner(user_id),
                purpose=f"state.{namespace}",
            )
        except BackendError as exc:
            if _is_missing(exc):
                return None
            raise
        if isinstance(body, dict):
            return body.get("value")
        return None

    async def delete_state(self, namespace: str, key: str, *, user_id: str) -> bool:
        try:
            resp = await self._write(
                "DELETE",
                f"/api/state/{namespace}/{key}",
                params=self._owner(user_id),
                purpose=f"state.{namespace}",
            )
        except BackendError as exc:
            if _is_missing(exc):
                return True  # 已经不在了，语义上等价于删成功
            raise
        try:
            return bool(resp.json().get("deleted"))
        except Exception:
            return True

    async def list_state(self, namespace: str, *, user_id: str) -> list[dict]:
        body = await self._json(
            "GET",
            f"/api/state/{namespace}",
            params=self._owner(user_id),
            purpose=f"state.{namespace}",
        )
        if isinstance(body, dict):
            return body.get("items") or []
        return body or []

    # ------------------------------------------------------------------
    # 订阅与路由（契约第 12 节）
    #
    # 这几条是"多用户"在 bot 侧的入口：来一条消息先问"谁要"（routing），
    # 抽一次，再按名单扇出；用户侧的 QQ 指令则直接读写订阅。
    # ------------------------------------------------------------------

    async def find_subscribers(self, group_id: str, sender_id: str | None = None) -> list[str]:
        """GET /api/subscriptions/routing → 这条消息要扇给哪些 user_id。

        bot 每处理一条消息都要问它一次。**空名单 = 没人要这条消息**，
        那就不该花 LLM 的钱去抽它（见 runner 里的 unsubscribed 分支）。

        `sender_id=None` = 这个群里任何发送者，只有缺口告警用（群级事件）。

        服务令牌专属（后端会拒用户令牌）：它返回的是全局投递名单。
        """
        params: list[tuple[str, Any]] = [("group_id", str(group_id))]
        if sender_id is not None:
            params.append(("sender_id", str(sender_id)))
        body = await self._json(
            "GET",
            "/api/subscriptions/routing",
            params=params,
            purpose="subscriptions.routing",
        )
        if isinstance(body, dict):
            return [str(u) for u in (body.get("user_ids") or []) if u]
        return []

    async def list_subscriptions(
        self, user_id: str, *, include_disabled: bool = True
    ) -> list[dict]:
        params = self._owner(user_id)
        if not include_disabled:
            params.append(("include_disabled", "false"))
        body = await self._json(
            "GET", "/api/subscriptions", params=params, purpose="subscriptions"
        )
        if isinstance(body, dict):
            return body.get("subscriptions") or []
        return body or []

    async def add_subscription(
        self,
        user_id: str,
        *,
        group_id: str,
        sender_id: str,
        group_name: str | None = None,
        sender_name: str | None = None,
        note: str | None = None,
    ) -> dict:
        """POST /api/subscriptions。

        `sender_id` 必填 —— 后端拒绝"订整个群"。所以这里不做任何兜底：
        指令层必须让用户明确给出发送者，否则宁可报错。
        """
        payload: dict = {"group_id": str(group_id), "sender_id": str(sender_id)}
        if group_name:
            payload["group_name"] = group_name
        if sender_name:
            payload["sender_name"] = sender_name
        if note:
            payload["note"] = note
        resp = await self._write(
            "POST",
            "/api/subscriptions",
            json=payload,
            params=self._owner(user_id),
            purpose="subscriptions.add",
            retries=0,  # 用户在线等
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def delete_subscription(self, user_id: str, sub_id: str) -> bool:
        resp = await self._write(
            "DELETE",
            f"/api/subscriptions/{sub_id}",
            params=self._owner(user_id),
            purpose="subscriptions.delete",
            retries=0,
        )
        try:
            return bool(resp.json().get("deleted"))
        except Exception:
            return True

    async def list_sources(self, *, keyword: str | None = None, limit: int = 200) -> list[dict]:
        """GET /api/sources → 信息源目录（这套部署见过的 (群, 发送者)）。

        给 QQ 侧的 `/订阅` 用：用户记不住群号，但认得群名和发送者名。
        """
        params: list[tuple[str, Any]] = [("limit", int(limit))]
        if keyword:
            params.append(("keyword", keyword))
        body = await self._json("GET", "/api/sources", params=params, purpose="sources")
        if isinstance(body, dict):
            return body.get("sources") or []
        return body or []

    # ------------------------------------------------------------------
    # 注册（契约第 3b 节）—— bot 只做两件事：签验证码、把码回给本人
    # ------------------------------------------------------------------

    async def request_verify_code(self, qq: str) -> dict:
        """POST /api/verify/request → {qq, code, expires_at, ...}。

        **只允许服务令牌调**，所以这一步只能由 bot 做。拿到码之后 bot 必须
        通过 QQ 回给本人 —— 这是整条注册链路的信任基础：只有能收到那条消息的
        人才证明得了自己拥有这个 QQ 号。前端拿不到这个接口，这是刻意的。
        """
        resp = await self._request(
            "POST",
            "/api/verify/request",
            json={"qq": str(qq)},
            purpose="verify.request",
            retries=0,  # 用户在线等
        )
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def list_users(self, *, limit: int = 500) -> list[dict]:
        """GET /api/users —— 所有用户（不含令牌摘要）。服务令牌专属。

        定时任务（digest / 盲区告警）需要遍历用户，而 bot 不允许自己持有
        用户名单（那会是跨重启的状态）。
        """
        body = await self._json("GET", "/api/users", purpose="users")
        if isinstance(body, dict):
            return body.get("users") or []
        return body or []

    async def get_user_by_qq(self, qq: str) -> dict | None:
        """GET /api/users/lookup?qq= → 用户，查不到返回 None。

        这是 bot 的**身份解析**入口：QQ 号是身份锚点，`user_id` 才是数据归属。
        404 翻译成 None（"这个人还没注册"），别的错误照抛 —— 把"没注册"
        和"后端挂了"混成一个 None，会让用户拿到一句莫名其妙的"稍后再试"。
        """
        try:
            body = await self._json(
                "GET",
                "/api/users/lookup",
                params=[("qq", str(qq))],
                purpose="users.lookup",
            )
        except BackendError as exc:
            if _is_missing(exc):
                return None
            raise
        if isinstance(body, dict):
            user = body.get("user")
            return user if isinstance(user, dict) else None
        return None

    # ------------------------------------------------------------------
    # 健康
    # ------------------------------------------------------------------

    async def health(self) -> dict:
        """GET /api/health —— **永远不抛异常**。

        状态页需要能显示"后端挂了"这件事本身：抛异常只会变成 500，
        前端就再也分不清"bot 挂了"和"后端挂了"。
        """
        result: dict[str, Any] = {
            "reachable": False,
            "base_url": self.settings.backend_base,
            "error": None,
        }
        try:
            resp = await self._request("GET", "/api/health", purpose="health", retries=0)
            body = resp.json()
        except BackendError as exc:
            result["error"] = str(exc)
            return result
        except Exception as exc:  # 非 JSON / 意外结构
            result["error"] = f"{type(exc).__name__}: {exc}"
            return result

        if not isinstance(body, dict):
            result["error"] = "后端返回了非对象结构"
            return result
        result.update(
            {
                "reachable": True,
                "ok": bool(body.get("ok")),
                "server_time": body.get("server_time"),
                "storage": body.get("storage"),
                "counts": body.get("counts"),
                "version": body.get("version"),
            }
        )
        return result
