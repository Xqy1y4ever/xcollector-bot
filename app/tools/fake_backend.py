"""本地假后端：按契约 docs/api.md 实现的**内存版后端**，用来在没有真后端时验证 bot。

    python -m app.tools.fake_backend --port 9000
    python -m app.tools.fake_backend --port 9000 --fail-first 2   # 前 2 次 POST /api/messages 返回 500
    python -m app.tools.fake_backend --port 9000 --api-token secret

**它是多用户的**：真后端把数据按 `user_id` 切开之后，这个假后端必须跟着切。
只给行打个 user_id 标签、读的时候不过滤，等于没切 —— 那正是"端到端测试在一个
不再像真后端的替身上全绿"，比没有测试更糟。切割规则整份抄自
backend/app/auth.py 的 `resolve_owner`：

  - 没配 `API_TOKEN`（`STATE.token == ""`）= 本地开发，不校验身份、一律按**服务令牌**放行；
  - 服务令牌 + 没带 `user_id` → **400**（唯一例外是存活探针 `GET /api/health`：
    不带 user_id 也放行，但 `counts` 返回 null —— Dockerfile 的 HEALTHCHECK 就是那么调的）；
  - 用户令牌（`POST /api/register` 签发的 `xc_...`）→ 归属**永远是它自己**，
    query 里的 `user_id` 一律忽略（否则改个参数就能读别人的数据）；
  - **按用户的数据**：notification / correction / read_state / gap_alert / stats /
    digest_log / bot_state / subscription。两个 user_id 之间绝不可见；
  - **共享层**（没有 user_id）：raw_message / attachment / group_state，所有人看到的是并集；
  - `notification` 的幂等键是 `(user_id, raw_message_id)`：同一条原文扇给两个人
    就是两行、两个 id（真后端的唯一约束就是这么建的）。

接口清单（`*` = 按用户，归属走 `user_id` query 参数；**没有任何按用户的字段在 body 里**）：

  raw_message   : POST/GET/PATCH /api/messages[/{id}]
  notification  : POST/GET/PATCH/DELETE /api/notifications[/{id}]*、corrections*、read*
  attachment    : POST /api/attachments（multipart）、GET /api/attachments/{id}（见下）
  group_state   : POST/GET /api/groups
  gap_alert     : POST/GET /api/gap-alerts*、POST /api/gap-alerts/{id}/ack*
  pipeline_stat : POST/GET /api/stats*
  digest_log    : POST/GET /api/digest-log*
  bot_state     : PUT/GET/DELETE /api/state/{ns}/{key}*、GET /api/state/{ns}*
  subscription  : GET/POST /api/subscriptions*、PATCH/DELETE /api/subscriptions/{id}*、
                  GET /api/subscriptions/routing（服务令牌专属）
  source        : GET /api/sources（从**共享层**聚合，不含任何按用户的数据）
  user          : POST /api/verify/request（服务令牌）、POST /api/register（**公开**）、
                  GET /api/me、GET /api/users[/lookup]、POST/GET /api/invites
  health        : GET /api/health

写接口一律**服务令牌专属**（403 给用户令牌）；`corrections` / `read` / 订阅是
用户能自己动的东西，用户令牌也放行 —— 与真后端的分工一致。

外加两个**只有假后端才有**的自检接口：
  GET  /api/_fake/state  —— 收到了什么、建了什么、调用序列
  POST /api/_fake/reset  —— 清空，便于一个进程里跑多组断言

`--fail-first N` 是专门为重试逻辑准备的：配上它跑
`python -m tests.check_pipeline_e2e`，就能在日志里看到 1s/2s/4s 的退避重试。

**附件签名 URL**：读投影里的 `attachments[].url` 每次现签
（`/api/attachments/{id}?exp=&u=&sig=`，HMAC 覆盖 user_id），下载接口认
"有效 Bearer"**或**"未过期的有效签名"。真后端在 signing.py 里做这件事，
这里只搬了最小的一份 —— 假后端原本完全没有这块逻辑，于是下载接口变成了
"谁都能拿"，那恰好是与真后端差得最远的一处。

注意它**没有**依赖 python-multipart（这台机器上装不上）：multipart 由
`_parse_multipart()` 手工解析，够读一个文件字段 + 几个普通字段。
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import logging
import random
import re
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

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

# 附件大小上限（契约：超限 413）
DEFAULT_MEDIA_MAX_BYTES = 5 * 1024 * 1024

# QQ 号形状。**和 backend/app/users.py 的 `_QQ_RE` 一个字都不能差** ——
# 订阅、注册、验证码三处都靠它把关，假后端放宽了就等于把真后端的入口校验测没了。
QQ_RE = re.compile(r"^[1-9]\d{4,11}$")

# 令牌前缀：一眼认出这是什么，也方便在日志/issue 里 grep。
TOKEN_PREFIX = "xc_"

# ---------------------------------------------------------------------------
# 可以原地翻转的开关
#
# 真后端这些是配置（SIGNUP_MODE / ALLOW_TOKEN_ROTATION / ATTACHMENT_URL_TTL）。
# 做成**模块级变量**是为了让自检脚本能在同进程里翻：
#
#     from app.tools import fake_backend as f
#     f.ALLOW_TOKEN_ROTATION = False      # 下一句断言 409
#     f.ATTACHMENT_URL_TTL = 0            # 下一句断言 url 退回裸路径
# ---------------------------------------------------------------------------

# 注册是否必须邀请码（真后端默认 SIGNUP_MODE=invite）
SIGNUPS_REQUIRE_INVITE = True
# 已有用户能否再走一次注册流程轮换令牌（关掉 → 409）
ALLOW_TOKEN_ROTATION = True
# QQ 验证码有效期与最多允许猜错几次
VERIFY_CODE_TTL_SECONDS = 600
VERIFY_MAX_ATTEMPTS = 5
# 附件签名有效期（秒）。0 = 不签名，退回"下载必须带 Bearer"
ATTACHMENT_URL_TTL = 3600

# 单用户订阅上限（挡住"批量订阅"这种用法，与 subscriptions.py 同值）
SUB_MAX_PER_USER = 200
_SUB_NOTE_MAX = 200
_SUB_NAME_MAX = 80
# 信息源目录一次最多返回多少条
SOURCE_LIMIT = 200

# 明确表达"整个群"的词。单独挡在这里，是为了给出一条能看懂的报错，
# 而不是让它掉进"QQ 号格式不对"里 —— 两者对用户的意思完全不同。
_GROUP_WIDE = {"*", "all", "any", "全部", "所有", "所有人", "全群", "整个群", "群里所有人"}

# 签名密钥的派生上下文（signing.py 的 _CONTEXT）：同一个令牌将来若还用于别的
# 签名用途，两边的密钥不会撞在一起。
_ATTACHMENT_SIGN_CONTEXT = b"xcollector-attachment-url-v1"
_SIG_LEN = 32


def _now_ms() -> int:
    return int(time.time() * 1000)


def _new_id() -> str:
    return f"{_now_ms():013d}{random.randint(0, 0xFFFFFF):06x}"


def _same_secret(left: str, right: str) -> bool:
    """定长比较令牌。转成 bytes 是为了容忍非 ASCII 的垃圾输入（compare_digest 会抛）。"""
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------


class MessageBody(BaseModel):
    # 除这几个之外的字段一律忽略（契约：未知字段忽略，不报错）。
    # 注意这里**故意比真后端宽松**（真后端 message_id/group_id/ts 是必填）：
    # 自检脚本会塞半截数据来造场景，收紧成 422 只会让假后端更难用。
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
    # 谁操作的（QQ 号或 "web"），**不是**租户 —— 租户从令牌/query 来。
    # 真后端里这个字段以前叫 user_id，多用户改造时改名成 actor 了：
    # 名字不改的话，"这条修正属于谁"和"谁改的"会被混成同一个东西。
    actor: str = "web"


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


class SubscriptionBody(BaseModel):
    group_id: str = ""
    # 必填，而且不接受 `*` 之类的通配符 —— 订阅的最小单位就是"某个群里某个人
    # 说的话"，没有"订整个群"这个选项（理由见 subscriptions.py）。
    sender_id: str = ""
    group_name: str | None = None
    sender_name: str | None = None
    note: str | None = None


class SubscriptionPatchBody(BaseModel):
    enabled: bool | None = None
    note: str | None = None
    group_name: str | None = None
    sender_name: str | None = None


class VerifyBody(BaseModel):
    qq: str = ""


class RegisterBody(BaseModel):
    qq: str = ""
    code: str = ""
    invite_code: str | None = None
    display_name: str | None = None


class InviteBody(BaseModel):
    note: str | None = None
    max_uses: int = 1
    ttl_seconds: int | None = None


# ---------------------------------------------------------------------------
# 身份
# ---------------------------------------------------------------------------


class Identity:
    """一次请求的调用者（auth.py 的 Identity 的内存版）。

    scope 只有三种：`service`（bot 的服务令牌）、`user`（注册签发的用户令牌）、
    `anonymous`（只有公开的注册接口会走到这个）。
    """

    __slots__ = ("scope", "user_id", "token")

    def __init__(self, scope: str, user_id: str | None = None, token: str = "") -> None:
        self.scope = scope
        self.user_id = user_id
        self.token = token

    @property
    def is_service(self) -> bool:
        return self.scope == "service"

    @property
    def is_user(self) -> bool:
        return self.scope == "user"

    def __repr__(self) -> str:  # pragma: no cover - 只为人肉调试
        return f"Identity(scope={self.scope!r}, user_id={self.user_id!r})"


SERVICE = Identity("service")
ANONYMOUS = Identity("anonymous")

# 全项目唯一允许不带 Authorization 头的接口（浏览器 <img src> 带不了那个头）
_ATTACHMENT_DOWNLOAD_RE = re.compile(r"^/api/attachments/[^/]+$")
# 公开接口：注册的前提就是"还没有令牌"，所以它天然在鉴权之外，
# 安全性由邀请码 + QQ 验证码担着。
_PUBLIC_PATHS = {"/api/register"}


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------


class State:
    """假后端的全部内存状态。

    多用户之后所有按用户的数据都**带 user_id 一起存**，读的时候一律先按
    user_id 过滤。键的形状列在下面，因为 tests/check_pipeline_e2e.py 会直接读它：

      messages / groups / attachment_blobs   共享层，没有用户概念
      notifications[notif_id]                行里带 user_id
      notif_by_raw[(user_id, raw_message_id)] 幂等索引（真后端的唯一约束）
      corrections[notif_id]                  notif_id 本身按用户唯一，所以不必再带
      reads                                  notif_id 集合，同上
      gap_alerts[gap_id]                     行里带 user_id
      stats[(user_id, day)]                  key 必须带 user_id：两个人同一天互不覆盖
      kv[(user_id, namespace, key)]          同上，真后端的 bot_state 主键就是这三个
      digest_logs                            列表，行里带 user_id
      subscriptions[sub_id]                  行里带 user_id
      users[user_id] / user_tokens[token] / user_index[qq]
      invites[code] / verify_codes[qq]

    `reset()` 会把上面**全部**清掉 —— 一个进程里跑多组断言靠的就是它。
    """

    def __init__(self) -> None:
        # 这几个是"进程级配置"，reset() 不动它们（否则每次 reset 都要重新设令牌）
        self.token: str = ""
        self.fail_first: int = 0
        self.fail_attachments: bool = False
        # 让 GET /api/subscriptions/routing 直接 500。
        #
        # 为什么需要它：投递名单查不动的时候，bot **绝不能**把它当成"没人要这条
        # 消息" —— 那会把本该建的通知永久吞掉，而且 raw 会被标成终态，
        # 连恢复循环都不会再试。这是"静默漏信息"最容易发生的一处，
        # 所以必须能真的注入这个故障来测它。
        #
        # 注意它和 token/fail_first 一样是**进程级**的：故意不放进 reset()，
        # 这样测试可以在一段流程中间打开、再关掉。
        self.fail_routing: bool = False
        self.media_max_bytes: int = DEFAULT_MEDIA_MAX_BYTES
        self.reset()

    def reset(self) -> None:
        self.post_message_calls = 0
        self.calls: list[dict] = []
        # ---- 共享层 ----
        self.messages: dict[str, dict] = {}
        self.message_index: dict[tuple[str, str], str] = {}
        self.patched_messages: list[dict] = []
        self.attachment_blobs: dict[str, dict] = {}
        self.groups: dict[str, dict] = {}
        # ---- 按用户 ----
        self.notifications: dict[str, dict] = {}
        # 键从 raw_message_id 变成 (user_id, raw_message_id)：同一条原文扇给两个人
        # 必须能同时存在两条通知，键里少了 user_id 就会互相顶掉。
        self.notif_by_raw: dict[tuple[str, str], str] = {}
        self.corrections: dict[str, dict] = {}
        self.correction_rows: list[dict] = []
        self.reads: set[str] = set()
        self.gap_alerts: dict[str, dict] = {}
        self.stats: dict[tuple[str, str], dict] = {}
        self.digest_logs: list[dict] = []
        self.kv: dict[tuple[str, str, str], dict] = {}
        self.subscriptions: dict[str, dict] = {}
        # ---- 用户系统 ----
        self.users: dict[str, dict] = {}
        self.user_tokens: dict[str, str] = {}
        self.user_index: dict[str, str] = {}
        self.invites: dict[str, dict] = {}
        self.verify_codes: dict[str, dict] = {}
        # ---- 自检 ----
        self.blob_hits: list[str] = []


STATE = State()
app = FastAPI(title="Xcollector fake backend")


# ---------------------------------------------------------------------------
# 鉴权与归属
#
# 这三个函数的语义**逐字**抄自 backend/app/auth.py。它们的错误信息也是契约的
# 一部分：前端据此区分"重新登录"（401）和"你没这个权限"（403）。
# ---------------------------------------------------------------------------


def _identity(request: Request) -> Identity:
    return getattr(request.state, "identity", SERVICE)


def _resolve_bearer(request: Request) -> Identity | None:
    """把 Authorization 头解析成身份；解析不出来返回 None（**不抛**）。

    用户令牌要先过 `startswith("xc_")` 再查表，和真后端一样：这样"随便一串
    字符"不会在用户表里瞎撞。
    """
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer":
        return None
    token = token.strip()
    if not token:
        return None
    if _same_secret(token, STATE.token.strip()):
        return Identity("service", token=token)
    if not token.startswith(TOKEN_PREFIX):
        return None
    user_id = STATE.user_tokens.get(token)
    if user_id is None:
        return None
    user = STATE.users.get(user_id)
    if user is None or user.get("status") != "active":
        return None
    user["last_seen_at"] = _now_ms()  # 真后端在 require_token 里顺手记活跃时间
    return Identity("user", user_id=user_id, token=token)


def _require_service(request: Request) -> Identity:
    """只允许服务令牌（bot）调用的接口。

    挂在这些上面：入库、建条/改条/删条、附件上传、groups、gap-alerts、stats、
    digest-log、state 的写、签发验证码/邀请码、用户名单、投递名单。
    **刻意不挂**在 corrections / read / 订阅上 —— 那是用户自己能动的东西。
    """
    identity = _identity(request)
    if not identity.is_service:
        raise HTTPException(
            status_code=403,
            detail="该接口只允许服务端（bot）调用。用户令牌只能读写自己的数据。",
        )
    return identity


def _owner(request: Request, user_id: str | None) -> str:
    """定出这次请求动的是**谁的数据**（auth.resolve_owner 的镜像）。

    用户令牌 → 永远是自己（query 里的 user_id 被忽略，这正是关键）；
    服务令牌 → 必须显式给 user_id：调用方没说清要动谁，就报错，
               猜一个的后果比报错严重得多。
    """
    identity = _identity(request)
    if identity.is_user:
        if not identity.user_id:  # pragma: no cover - 构造上不可能
            raise HTTPException(status_code=401, detail="令牌无效")
        return identity.user_id

    owner = (user_id or "").strip()
    if not owner:
        raise HTTPException(
            status_code=400,
            detail="服务令牌必须显式指定 user_id：这次请求要动谁的数据？",
        )
    return owner


def _owner_optional(request: Request, user_id: str | None) -> str | None:
    """和 `_owner` 一样，但允许"这次请求不属于任何用户"。

    只有存活探针 `GET /api/health` 用它：Dockerfile 的 HEALTHCHECK 只带
    API_TOKEN 打这个接口，拿不到 user_id。调用方拿到 None 之后**必须真的不查
    任何用户数据**（counts 返回 null），而不是拿 None 去查。
    """
    identity = _identity(request)
    if identity.is_user:
        return _owner(request, user_id)
    return (user_id or "").strip() or None


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


def _flag(value: str | None, *, default: bool = False) -> bool:
    """把 `count_only=1` / `include_disabled=false` 这类开关解析成 bool。"""
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on", "t")


def _optional_flag(value: str | None) -> bool | None:
    """三态开关：不传 = 不过滤。"""
    if value is None or str(value).strip() == "":
        return None
    return _flag(value)


@app.middleware("http")
async def record_and_auth(request: Request, call_next):
    """记录每一次调用，并把"谁在调"解析成 `request.state.identity`。

    鉴权分三档，和真后端的三个 router 一一对应：

      - 没配 API_TOKEN → 不校验，一律服务令牌（本地开发模式）；
      - `/api/register` → 公开，谁都能调（注册的前提就是还没有令牌）；
      - `GET /api/attachments/{id}` → **唯一允许不带 Authorization 头**的接口，
        这里不拦，交给路由判断"有效 Bearer 或有效签名"；
      - 其余 → 必须有有效令牌，否则 401（缺头 vs 令牌不对，文案分开，
        前端据此决定是"重新登录"还是别的）。
    """
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

    path = request.url.path
    identity: Identity | None
    if not STATE.token:
        identity = SERVICE
    elif path in _PUBLIC_PATHS:
        identity = ANONYMOUS
    elif _ATTACHMENT_DOWNLOAD_RE.match(path) and request.method == "GET":
        identity = _resolve_bearer(request)
    else:
        identity = _resolve_bearer(request)
        if identity is None:
            entry["status"] = 401
            has_bearer = request.headers.get("authorization", "").lower().startswith("bearer")
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "令牌无效" if has_bearer else "缺少 Authorization: Bearer <令牌>"
                },
            )

    request.state.identity = identity if identity is not None else ANONYMOUS
    entry["scope"] = request.state.identity.scope
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
# 附件签名 URL（signing.py 的最小镜像）
#
# 为什么需要：读投影里的 `attachment.url` 会被前端直接塞进 `<img src>`，
# 而那个标签带不了 Authorization 头。所以 URL 里带 exp + HMAC，**而且绑定
# user_id** —— 不绑的话，拿到别人通知里那条链接的人就能看别人的附件，
# 而那条链接本身看起来完全正常。
# ---------------------------------------------------------------------------


def _sign_key() -> bytes:
    """签名密钥；没有可用密钥（本地开发）时返回空 = 不签名。"""
    secret = (STATE.token or "").strip()
    if not secret:
        return b""
    return hmac.new(_ATTACHMENT_SIGN_CONTEXT, secret.encode("utf-8"), hashlib.sha256).digest()


def _signature(key: bytes, user_id: str, att_id: str, exp: int) -> str:
    return hmac.new(
        key, f"{user_id}.{att_id}.{exp}".encode("utf-8"), hashlib.sha256
    ).hexdigest()[:_SIG_LEN]


def _attachment_path(att_id: str) -> str:
    """附件的规范路径（不带过期时间的裸路径）。上传响应与库里存的都是它 ——
    存签名会过期，历史条目的图就打不开了。"""
    return f"/api/attachments/{quote(str(att_id), safe='')}"


def _sign_attachment_url(user_id: str, att_id: str) -> str:
    base = _attachment_path(att_id)
    key = _sign_key()
    try:
        ttl = int(ATTACHMENT_URL_TTL or 0)
    except (TypeError, ValueError):  # pragma: no cover - 只有配置写错才会走到
        ttl = 0
    if not key or ttl <= 0:
        return base
    exp = int(time.time()) + ttl
    # `u=` 必须带上：下载请求没有 Authorization 头，验证方只能从 URL 里知道
    # "这条链接是给谁的"。它进了签名内容，改不动。
    return (
        f"{base}?exp={exp}&u={quote(str(user_id), safe='')}"
        f"&sig={_signature(key, str(user_id), str(att_id), exp)}"
    )


def _verify_attachment_sig(user_id: str, att_id: str, exp: Any, sig: Any) -> bool:
    """校验签名、归属与过期。任何异常都返回 False（**失败即拒绝**）。"""
    key = _sign_key()
    if not key:
        return False
    try:
        exp_i = int(str(exp))
    except (TypeError, ValueError):
        return False
    if exp_i < int(time.time()):
        return False
    want = _signature(key, str(user_id), str(att_id), exp_i)
    try:
        return hmac.compare_digest(want.encode("utf-8"), str(sig or "").encode("utf-8"))
    except Exception:  # pragma: no cover
        return False


def _attachment_id_of(item: dict) -> str:
    """从附件 dict 里取 id；没有 id 时退回从 url 里抠（兼容老数据）。"""
    att_id = item.get("id")
    if att_id:
        return str(att_id)
    url = item.get("url")
    if isinstance(url, str) and "/attachments/" in url:
        tail = url.split("/attachments/", 1)[1]
        return tail.split("?", 1)[0].split("/", 1)[0]
    return ""


def _sign_attachments(user_id: str, items: Any) -> list[dict]:
    """把附件列表里的 `url` 换成现签的、**绑定该用户**的签名 URL。

    库里存的始终是裸路径，签名只在读投影这一步加 —— 所以前端什么都不用改。
    """
    if not isinstance(items, list):
        return []
    out: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            out.append(item)
            continue
        att_id = _attachment_id_of(item)
        if not att_id:
            out.append(dict(item))
            continue
        out.append({**item, "url": _sign_attachment_url(user_id, att_id)})
    return out


# ---------------------------------------------------------------------------
# 读投影
# ---------------------------------------------------------------------------


def _effective(notif: dict) -> dict:
    """通知的读投影：correction 覆盖 notification，再由 due_at 推导 status。

    **不含 user_id**（和真后端一样：调用方已经知道这是谁的）。
    """
    # notif_id 在全局唯一，而它只可能被建立它的那个用户读到（所有入口都过了
    # 归属检查），所以修正按 notif_id 取不会串。
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
        # 附件从**共享**的 raw_message 取，但链接按当前通知的归属现签：
        # 同一条原文扇给两个人，两个人拿到的链接不能互相通用。
        "attachments": _sign_attachments(
            str(notif.get("user_id") or ""), raw.get("attachments") or []
        ),
        "extractor": notif.get("extractor"),
        "model": notif.get("model"),
        "prompt_ver": notif.get("prompt_ver"),
        "source_ts": notif.get("source_ts"),
        "created_at": notif.get("created_at"),
        "updated_at": notif.get("updated_at"),
    }


def _raw_view(row: dict, user_id: str = "") -> dict:
    """原始消息的对外形状。

    raw_message 是**共享**的（它属于所有订阅了这条消息的人），但附件链接必须
    只对请求者有效 —— 所以 `user_id` 只用来签链接，不做过滤。
    真后端的 `GET /api/messages` 不带 user_id，签名就绑在空串上，这里照抄。
    """
    return {
        **row,
        "attachments": _sign_attachments(user_id, row.get("attachments") or []),
    }


def _state_live(row: dict | None) -> dict | None:
    if row is None:
        return None
    expires = row.get("expires_at")
    if expires is not None and int(expires) <= _now_ms():
        return None  # 读取时判过期是必须的，清理只是省空间
    return row


def public_user(row: dict | None) -> dict | None:
    """对外的用户形状。**绝不包含令牌或它的摘要。**"""
    if row is None:
        return None
    return {
        "id": row.get("id"),
        "qq": row.get("qq"),
        "display_name": row.get("display_name"),
        "token_hint": row.get("token_hint"),
        "status": row.get("status"),
        "created_at": row.get("created_at"),
        "last_seen_at": row.get("last_seen_at"),
    }


def public_subscription(row: dict | None) -> dict:
    """对外的订阅形状。**不含 user_id** —— 调用方已经知道那是谁了。"""
    row = row or {}
    return {
        "id": str(row.get("id") or ""),
        "group_id": str(row.get("group_id") or ""),
        "sender_id": str(row.get("sender_id") or ""),
        "group_name": row.get("group_name"),
        "sender_name": row.get("sender_name"),
        "note": row.get("note"),
        "enabled": bool(row.get("enabled")),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


# ---------------------------------------------------------------------------
# 1. raw_message（共享层）
# ---------------------------------------------------------------------------


@app.post("/api/messages")
async def create_message(body: MessageBody, request: Request):
    _require_service(request)
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
    count_only: str | None = Query(default=None),
):
    """共享层：所有人的原文是并集，**没有** user_id 过滤（真后端也没这个参数）。"""
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
    if _flag(count_only):
        return {"count": len(rows)}
    # 真后端这里不带 user_id，签名就绑在空串上；照抄，别自作聪明。
    return {"messages": [_raw_view(row) for row in rows[:limit]]}


@app.get("/api/messages/{raw_id}")
async def get_message(raw_id: str):
    row = STATE.messages.get(raw_id)
    if row is None:
        raise HTTPException(status_code=404, detail="消息不存在")
    return _raw_view(row)


@app.patch("/api/messages/{raw_id}")
async def patch_message(raw_id: str, body: MessagePatchBody, request: Request):
    _require_service(request)
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
    return _raw_view(row)


# ---------------------------------------------------------------------------
# 2. notification（按用户）
# ---------------------------------------------------------------------------

# 允许 PATCH 的机器字段（契约 §2）。status / read 刻意不在里面：
# 它们必须走 corrections 与 /read 留痕。
_NOTIFICATION_PATCHABLE = (
    "title",
    "summary",
    "location",
    "due_at",
    "due_text",
    "due_confidence",
    "evidence",
    "conflict",
    "candidates",
    "model",
    "prompt_ver",
)


@app.post("/api/notifications")
async def create_notification(body: NotificationBody, request: Request, user_id: str | None = Query(default=None)):
    """创建或更新通知（幂等：`(user_id, raw_message_id)` 唯一）。

    **按用户扇出**：同一条 raw_message 被 N 个人订阅，bot 就在这里写 N 次、
    每次带不同的 user_id。抽取只跑一次，这里只是把结果分发出去。
    """
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    if not (body.evidence or "").strip():
        # 契约：由后端替 bot 守住"没有证据的条目不许入库"（防的是模型幻觉）
        raise HTTPException(status_code=400, detail="没有证据的条目不许入库：evidence 不能为空")

    existing = STATE.notif_by_raw.get((owner, body.raw_message_id))
    if existing:
        row = STATE.notifications[existing]
        # 只覆盖机器字段：人工修正存在 corrections 里，重跑抽取永远冲不掉它
        for field in _NOTIFICATION_PATCHABLE:
            row[field] = getattr(body, field)
        row["updated_at"] = _now_ms()
        logger.info("通知已存在（幂等更新）notif=%s user=%s raw=%s", existing, owner, body.raw_message_id)
        return {"id": existing, "created": False}

    notif_id = _new_id()
    row = body.model_dump()
    row["id"] = notif_id
    row["user_id"] = owner
    row["created_at"] = _now_ms()
    row["updated_at"] = _now_ms()
    STATE.notifications[notif_id] = row
    STATE.notif_by_raw[(owner, body.raw_message_id)] = notif_id
    logger.info(
        "已建通知 notif=%s user=%s 标题=%s 截止=%s 来源群=%s",
        notif_id,
        owner,
        body.title,
        body.due_text or body.due_at,
        body.group_id,
    )
    return {"id": notif_id, "created": True}


def _owned_notification(notif_id: str, owner: str) -> dict:
    """取一条属于 owner 的通知，不是他的一律 404。

    404 而不是 403：不然可以用它探测"这个 id 存在吗"。
    """
    row = STATE.notifications.get(notif_id)
    if row is None or row.get("user_id") != owner:
        raise HTTPException(status_code=404, detail="通知不存在")
    return row


@app.get("/api/notifications")
async def list_notifications(
    request: Request,
    user_id: str | None = Query(default=None),
    since: int | None = Query(default=None),
    status: str = Query(default="all"),
    q: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    count_only: str | None = Query(default=None),
):
    owner = _owner(request, user_id)
    views = [
        _effective(n) for n in STATE.notifications.values() if n.get("user_id") == owner
    ]
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
    if _flag(count_only):
        return {"count": len(views)}
    return {"notifications": views[:limit], "server_time": _now_ms()}


@app.get("/api/notifications/{notif_id}")
async def get_notification(notif_id: str, request: Request, user_id: str | None = Query(default=None)):
    owner = _owner(request, user_id)
    row = _owned_notification(notif_id, owner)
    raw = STATE.messages.get(row.get("raw_message_id") or "")
    return {
        "notification": _effective(row),
        # 原始消息是共享的，但附件链接绑到当前用户（真后端的 raw_view(raw, owner)）
        "raw": _raw_view(raw, owner) if raw else {},
    }


@app.patch("/api/notifications/{notif_id}")
async def patch_notification(
    notif_id: str, body: NotificationPatchBody, request: Request, user_id: str | None = Query(default=None)
):
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, body.model_dump(exclude_none=True))
    row = _owned_notification(notif_id, owner)
    if body.evidence is not None and not str(body.evidence).strip():
        raise HTTPException(status_code=400, detail="没有证据的条目不许入库：evidence 不能为空")
    # status / read 不允许在这里改（只能走 corrections / read），传了忽略
    for field, value in body.model_dump(exclude_none=True).items():
        row[field] = value
    row["updated_at"] = _now_ms()
    return _effective(row)


@app.delete("/api/notifications/{notif_id}")
async def delete_notification(notif_id: str, request: Request, user_id: str | None = Query(default=None)):
    _require_service(request)
    owner = _owner(request, user_id)
    row = _owned_notification(notif_id, owner)
    STATE.notifications.pop(notif_id, None)
    STATE.notif_by_raw.pop((owner, row.get("raw_message_id") or ""), None)
    # correction / read_state 刻意**不级联删除**（真后端：只追加层不做级联删除）
    return {"deleted": True}


@app.post("/api/notifications/{notif_id}/corrections")
async def corrections(
    notif_id: str, body: CorrectionBody, request: Request, user_id: str | None = Query(default=None)
):
    """人工修正（只追加）。这是**用户**能做的写操作之一，所以不要求服务令牌。"""
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    if body.field not in {"title", "summary", "location", "due_at", "due_text", "status"}:
        raise HTTPException(status_code=400, detail=f"field 非法：{body.field}")
    if body.field == "status" and body.value not in ("active", "archived", "done"):
        raise HTTPException(status_code=400, detail=f"status 非法：{body.value}")
    _owned_notification(notif_id, owner)

    value = body.value
    if body.field == "due_at" and value in ("", "null"):
        value = None
    STATE.corrections.setdefault(notif_id, {})[body.field] = value
    STATE.correction_rows.append(
        {
            "id": _new_id(),
            "notification_id": notif_id,
            "user_id": owner,
            "field": body.field,
            "value": None if value is None else str(value),
            # actor 是"谁改的"，user_id 是"这是谁的数据"，两个都留着
            "actor": body.actor or "web",
            "ts": _now_ms(),
        }
    )
    row = STATE.notifications.get(notif_id)
    if row is not None:
        row["updated_at"] = _now_ms()  # since 是"变过没有"的游标，修正也要推一下
    logger.info(
        "修正 notif=%s user=%s field=%s value=%s by=%s",
        notif_id,
        owner,
        body.field,
        value,
        body.actor,
    )
    return {"ok": True, "notification": _effective(row) if row else None}


@app.get("/api/notifications/{notif_id}/corrections")
async def list_corrections(notif_id: str, request: Request, user_id: str | None = Query(default=None)):
    owner = _owner(request, user_id)
    _owned_notification(notif_id, owner)
    rows = [
        {
            "id": r["id"],
            "notification_id": r["notification_id"],
            "field": r["field"],
            "value": r["value"],
            "actor": r["actor"],
            "ts": r["ts"],
        }
        for r in STATE.correction_rows
        if r["notification_id"] == notif_id and r["user_id"] == owner
    ]
    rows.sort(key=lambda r: (r["ts"], r["id"]))
    return {"corrections": rows}


@app.post("/api/notifications/{notif_id}/read")
async def mark_read(
    notif_id: str, request: Request, body: ReadBody | None = None, user_id: str | None = Query(default=None)
):
    owner = _owner(request, user_id)
    _owned_notification(notif_id, owner)
    want = True if body is None else bool(body.read)
    if want:
        STATE.reads.add(notif_id)
    else:
        STATE.reads.discard(notif_id)
    row = STATE.notifications.get(notif_id)
    if row is not None:
        row["updated_at"] = _now_ms()
    return {"read": want}


# ---------------------------------------------------------------------------
# 3. attachment（共享层，但下载鉴权特殊）
# ---------------------------------------------------------------------------


@app.post("/api/attachments")
async def upload_attachment(request: Request):
    _require_service(request)
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
        # 上传响应给的是**裸路径**：签名会过期，存下来历史条目就打不开了
        "url": _attachment_path(att_id),
        "size": len(content),
        "content_type": STATE.attachment_blobs[att_id]["content_type"],
    }


@app.get("/api/attachments/{att_id}")
async def download_attachment(
    att_id: str,
    request: Request,
    exp: str | None = Query(default=None),
    u: str | None = Query(default=None),
    sig: str | None = Query(default=None),
):
    """**全项目唯一允许不带 Authorization 头的接口。**

    放行条件（任一满足即可，与真后端的 require_download 一致）：
      1. 没配 API_TOKEN（本地开发，与其它接口一致地不校验）
      2. 带了有效 Bearer
      3. `u` + `exp` + `sig` 签名有效且未过期
    都不满足 → 401。
    """
    identity = _identity(request)
    if STATE.token and not (identity.is_service or identity.is_user):
        if not (u and _verify_attachment_sig(u, att_id, exp, sig)):
            raise HTTPException(
                status_code=401,
                detail="附件需要有效的 Authorization: Bearer <令牌>，或未过期的签名链接",
            )
    blob = STATE.attachment_blobs.get(att_id)
    if blob is None:
        raise HTTPException(status_code=404, detail="附件不存在")
    return Response(
        content=blob["content"],
        media_type=blob["content_type"],
        headers={
            "Content-Disposition": f'inline; filename="{blob["filename"]}"',
            "Content-Length": str(len(blob["content"])),
            # 附件内容来自外部，绝不让浏览器按嗅探出来的类型执行它
            "X-Content-Type-Options": "nosniff",
        },
    )


# ---------------------------------------------------------------------------
# 4. group_state（共享层）
# ---------------------------------------------------------------------------


@app.post("/api/groups")
async def upsert_group(body: GroupBody, request: Request):
    _require_service(request)
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
# 5. gap_alert（按用户）
# ---------------------------------------------------------------------------


@app.post("/api/gap-alerts")
async def create_gap_alert(body: GapAlertBody, request: Request, user_id: str | None = Query(default=None)):
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    gap_id = "gap_" + _new_id()[13:]
    STATE.gap_alerts[gap_id] = {
        "id": gap_id,
        "user_id": owner,
        "group_id": body.group_id,
        "group_name": body.group_name,
        "from_ts": body.from_ts,
        "to_ts": body.to_ts,
        "reason": body.reason,
        "acknowledged": False,
        "created_at": _now_ms(),
    }
    logger.info("缺口告警 %s user=%s 群=%s %s", gap_id, owner, body.group_id, body.reason)
    return {"id": gap_id}


@app.get("/api/gap-alerts")
async def list_gap_alerts(
    request: Request,
    user_id: str | None = Query(default=None),
    acknowledged: str | None = Query(default=None),
    limit: int = Query(default=20),
):
    owner = _owner(request, user_id)
    rows = [dict(r) for r in STATE.gap_alerts.values() if r.get("user_id") == owner]
    want = _optional_flag(acknowledged)
    if want is not None:
        rows = [r for r in rows if bool(r["acknowledged"]) == want]
    rows.sort(key=lambda r: -(r.get("created_at") or 0))
    return {"alerts": rows[:limit]}


@app.post("/api/gap-alerts/{gap_id}/ack")
async def ack_gap_alert(gap_id: str, request: Request, user_id: str | None = Query(default=None)):
    _require_service(request)
    owner = _owner(request, user_id)
    row = STATE.gap_alerts.get(gap_id)
    if row is None or row.get("user_id") != owner:
        raise HTTPException(status_code=404, detail="缺口告警不存在")
    row["acknowledged"] = True
    return {"acknowledged": True}


# ---------------------------------------------------------------------------
# 6. pipeline_stat（按用户）
# ---------------------------------------------------------------------------


@app.post("/api/stats")
async def add_stats(body: StatsBody, request: Request, user_id: str | None = Query(default=None)):
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    day = body.day or _day_of(_now_ms())
    # 统计必须按用户分开：一条消息被扇给 N 个人，就给这 N 个人各记一次。
    # key 少了 user_id，两个人的"今天处理了几条"会互相累加。
    row = STATE.stats.setdefault((owner, day), {"day": day})
    for field, value in (body.fields or {}).items():
        try:
            row[field] = int(row.get(field) or 0) + int(value)
        except (TypeError, ValueError):
            row[field] = value
    return row


@app.get("/api/stats")
async def get_stats(request: Request, user_id: str | None = Query(default=None), day: str | None = Query(default=None)):
    owner = _owner(request, user_id)
    key = day or _day_of(_now_ms())
    return STATE.stats.get((owner, key), {"day": key})


# ---------------------------------------------------------------------------
# 10. digest_log（按用户）
# ---------------------------------------------------------------------------


@app.post("/api/digest-log")
async def add_digest_log(body: DigestLogBody, request: Request, user_id: str | None = Query(default=None)):
    """记录一次发送。幂等键 `(user_id, day, kind, sent)`。

    幂等是刻意的：bot 重启/重试时重复提交不该让"今天发过没有"的答案变成 2 ——
    对收件人来说重发一条摘要比漏发更糟。
    """
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    day = body.day or _day_of(_now_ms())
    sent = bool(body.sent)
    for row in STATE.digest_logs:
        if (row["user_id"], row["day"], row["kind"], bool(row["sent"])) == (owner, day, body.kind, sent):
            return {"id": row["id"]}
    log_id = _new_id()
    STATE.digest_logs.append(
        {
            "id": log_id,
            "user_id": owner,
            "day": day,
            "kind": body.kind,
            "text": body.text,
            "sent": sent,
            "error": body.error,
            "ts": _now_ms(),
        }
    )
    return {"id": log_id}


@app.get("/api/digest-log")
async def list_digest_logs(
    request: Request,
    user_id: str | None = Query(default=None),
    day: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    sent: str | None = Query(default=None),
    limit: int = Query(default=50),
    count_only: str | None = Query(default=None),
):
    owner = _owner(request, user_id)
    rows = [dict(r) for r in STATE.digest_logs if r.get("user_id") == owner]
    if day:
        rows = [r for r in rows if r["day"] == day]
    if kind:
        rows = [r for r in rows if r["kind"] == kind]
    want = _optional_flag(sent)
    if want is not None:
        rows = [r for r in rows if bool(r["sent"]) == want]
    rows.sort(key=lambda r: -(r.get("ts") or 0))
    if _flag(count_only):
        return {"count": len(rows)}
    return {"logs": rows[:limit]}


# ---------------------------------------------------------------------------
# 11. bot_state（按用户）
# ---------------------------------------------------------------------------


@app.put("/api/state/{namespace}/{key}")
async def put_state(
    namespace: str, key: str, body: StateBody, request: Request, user_id: str | None = Query(default=None)
):
    _require_service(request)
    owner = _owner(request, user_id)
    _note(request, {"value": body.value, "ttl_seconds": body.ttl_seconds, "user_id": owner})
    expires_at = None
    if body.ttl_seconds is not None:
        expires_at = _now_ms() + int(body.ttl_seconds) * 1000
    STATE.kv[(owner, namespace, key)] = {
        "key": key,
        "user_id": owner,
        "value": body.value,
        "expires_at": expires_at,
        "updated_at": _now_ms(),
    }
    logger.info("bot_state 写入 user=%s %s/%s ttl=%s", owner, namespace, key, body.ttl_seconds)
    return {"ok": True, "expires_at": expires_at}


@app.get("/api/state/{namespace}/{key}")
async def get_state(namespace: str, key: str, request: Request, user_id: str | None = Query(default=None)):
    owner = _owner(request, user_id)
    row = _state_live(STATE.kv.get((owner, namespace, key)))
    if row is None:
        raise HTTPException(status_code=404, detail="不存在或已过期")
    return {"key": key, "value": row["value"], "expires_at": row.get("expires_at")}


@app.delete("/api/state/{namespace}/{key}")
async def delete_state(namespace: str, key: str, request: Request, user_id: str | None = Query(default=None)):
    _require_service(request)
    owner = _owner(request, user_id)
    existed = STATE.kv.pop((owner, namespace, key), None) is not None
    return {"deleted": existed}


@app.get("/api/state/{namespace}")
async def list_state(
    namespace: str,
    request: Request,
    user_id: str | None = Query(default=None),
    count_only: str | None = Query(default=None),
):
    owner = _owner(request, user_id)
    items = []
    for (uid, ns, key), row in list(STATE.kv.items()):
        if uid != owner or ns != namespace:
            continue
        if _state_live(row) is None:
            continue
        items.append({"key": key, "value": row["value"], "expires_at": row.get("expires_at")})
    if _flag(count_only):
        return {"count": len(items)}
    return {"items": items}


# ---------------------------------------------------------------------------
# 12. source（共享层聚合出来的信息源目录）
# ---------------------------------------------------------------------------


@app.get("/api/sources")
async def list_sources(
    request: Request,
    keyword: str | None = Query(default=None),
    limit: int = Query(default=SOURCE_LIMIT, ge=1, le=1000),
):
    """**信息源目录**：这套部署见过的 (群, 发送者) 组合。

    新用户注册后手上是空的 —— 没有通知、不知道群号，也就无从订阅，所以需要
    一份目录让他能挑。它回答的是"这套部署看得见哪些来源"，而不是"谁收了多少"，
    因此**从共享的 raw_message 聚合**，结果里绝不能出现 user_id
    （出现就是按用户的数据泄露）。

    代价要说清楚：只有 bot **实际处理过**的组合才会出现在这里。
    """
    _ = _identity(request)  # 目录与身份无关，但必须登录
    agg: dict[tuple[str, str], dict] = {}
    for row in STATE.messages.values():
        gid = str(row.get("group_id") or "")
        sid = str(row.get("sender_id") or "")
        if not gid or not sid:
            continue
        item = agg.get((gid, sid))
        ts = int(row.get("ts") or 0)
        if item is None:
            agg[(gid, sid)] = {
                "group_id": gid,
                "group_name": row.get("group_name"),
                "sender_id": sid,
                "sender_name": row.get("sender_name"),
                "last_ts": ts,
                "msg_count": 1,
            }
            continue
        if row.get("group_name"):
            item["group_name"] = row["group_name"]
        if row.get("sender_name"):
            item["sender_name"] = row["sender_name"]
        item["msg_count"] += 1
        item["last_ts"] = max(int(item.get("last_ts") or 0), ts)

    rows = list(agg.values())
    needle = (keyword or "").strip().lower()
    if needle:
        rows = [
            r
            for r in rows
            if any(
                needle in str(r.get(field) or "").lower()
                for field in ("group_name", "sender_name", "group_id", "sender_id")
            )
        ]
    rows.sort(key=lambda r: -(r.get("last_ts") or 0))
    rows = rows[:limit]
    return {"sources": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# 7. health
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health(request: Request, user_id: str | None = Query(default=None)):
    """只报存储自身，计数按用户算。

    服务令牌可以**不带** user_id —— 那就是纯存活探针（Dockerfile 的 HEALTHCHECK
    就是这么调的，它拿不到 user_id）。那种情况下 counts 直接返回 None，而不是
    去查"所有用户"：探针不该顺带做一次无归属查询。
    """
    owner = _owner_optional(request, user_id)
    counts = None
    if owner is not None:
        counts = {
            # messages / attachments 在共享层，天然是全集；notifications 按用户数
            "messages": len(STATE.messages),
            "notifications": sum(
                1 for n in STATE.notifications.values() if n.get("user_id") == owner
            ),
            "attachments": len(STATE.attachment_blobs),
        }
    return {
        "ok": True,
        "server_time": _now_ms(),
        "user_id": owner,
        "storage": {"driver": "memory", "path": ":fake:", "writable": True},
        "counts": counts,
        "version": "0.2.0-fake",
    }


# ---------------------------------------------------------------------------
# 9. 订阅（按用户）+ 投递名单（服务令牌专属）
# ---------------------------------------------------------------------------


def _normalize_group(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="必须指定群号")
    if not QQ_RE.match(text):
        raise HTTPException(
            status_code=400, detail=f"群号不像个 QQ 群号：{text!r}（应该是 5~12 位数字，且不以 0 开头）"
        )
    return text


def _normalize_sender(raw: Any) -> str:
    """校验发送者。**"禁止整个群"就落在这个函数里。**

    三层堵死（空值 / 通配符 / 逗号分隔的多值），理由见 subscriptions.py：
    订阅的最小单位是"某个群里某个人说的话"，一旦允许"订这个群"，
    LLM 调用量和误报会一起失控。
    """
    text = str(raw or "").strip()
    if not text:
        raise HTTPException(
            status_code=400,
            detail="必须指定发送者 QQ 号：订阅的最小单位是「某个群里某个人说的话」，不支持订阅整个群",
        )
    if text.lower() in _GROUP_WIDE:
        raise HTTPException(
            status_code=400,
            detail=f"不支持订阅整个群（sender_id={text!r}）：这样会把这个群里所有人的发言都算进来。"
            "请填发出通知的那个人的 QQ 号",
        )
    if "," in text or "，" in text:
        raise HTTPException(status_code=400, detail="一次只能订一个发送者，请分开添加")
    if not QQ_RE.match(text):
        raise HTTPException(
            status_code=400,
            detail=f"发送者 QQ 号不像个 QQ 号：{text!r}（应该是 5~12 位数字，且不以 0 开头）",
        )
    return text


def _clean_text(value: Any, *, field: str, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > limit:
        raise HTTPException(status_code=400, detail=f"{field}最多 {limit} 个字")
    return text


def _owned_subscription(sub_id: str, owner: str) -> dict:
    row = STATE.subscriptions.get(sub_id)
    if row is None or row.get("user_id") != owner:
        # 404 而不是 403：别人的 id 探测不出来（和通知详情一致）
        raise HTTPException(status_code=404, detail="订阅不存在")
    return row


@app.get("/api/subscriptions")
async def list_subscriptions(
    request: Request,
    user_id: str | None = Query(default=None),
    include_disabled: str | None = Query(default=None),
):
    """我的订阅。用户令牌看自己，服务令牌要显式带 user_id。"""
    owner = _owner(request, user_id)
    rows = [r for r in STATE.subscriptions.values() if r.get("user_id") == owner]
    if include_disabled is not None and not _flag(include_disabled, default=True):
        rows = [r for r in rows if r["enabled"]]
    rows.sort(key=lambda r: (-(r.get("updated_at") or 0), r["id"]))
    return {"subscriptions": [public_subscription(r) for r in rows], "count": len(rows)}


@app.post("/api/subscriptions")
async def add_subscription(
    body: SubscriptionBody, request: Request, user_id: str | None = Query(default=None)
):
    """订一个 (群, 发送者)。

    **刻意不要求服务令牌**：这是用户自己配置自己的东西，用户令牌就该能改
    （和 corrections / read 一致）。服务令牌也能调，但必须显式带 user_id ——
    那是 bot 处理 QQ 侧 `/订阅` 指令时用的路径。

    用户令牌调用时，query 里的 user_id 被忽略、强制用令牌自己的归属：
    否则用户换个参数就能替别人订阅。
    """
    owner = _owner(request, user_id)
    _note(request, {**body.model_dump(), "user_id": owner})
    group = _normalize_group(body.group_id)
    sender = _normalize_sender(body.sender_id)
    clean = {
        "group_name": _clean_text(body.group_name, field="群名", limit=_SUB_NAME_MAX),
        "sender_name": _clean_text(body.sender_name, field="发送者备注", limit=_SUB_NAME_MAX),
        "note": _clean_text(body.note, field="备注", limit=_SUB_NOTE_MAX),
    }

    existing = next(
        (
            r
            for r in STATE.subscriptions.values()
            if r["user_id"] == owner and r["group_id"] == group and r["sender_id"] == sender
        ),
        None,
    )
    if existing is None:
        # 上限只在**真的要新增**时检查：把一条已有的重新打开不该被上限挡住
        count = sum(1 for r in STATE.subscriptions.values() if r["user_id"] == owner)
        if count >= SUB_MAX_PER_USER:
            raise HTTPException(
                status_code=400, detail=f"订阅数已达上限 {SUB_MAX_PER_USER} 条，先删掉一些再加"
            )
        sub_id = "sub_" + _new_id()[13:]
        stamp = _now_ms()
        row = {
            "id": sub_id,
            "user_id": owner,
            "group_id": group,
            "sender_id": sender,
            "group_name": clean["group_name"],
            "sender_name": clean["sender_name"],
            "note": clean["note"],
            "enabled": True,
            "created_at": stamp,
            "updated_at": stamp,
        }
        STATE.subscriptions[sub_id] = row
        logger.info("新订阅 sub=%s user=%s (%s, %s)", sub_id, owner, group, sender)
        return {"subscription": public_subscription(row), "created": True}

    # 重复订阅不报错：这和"把一条关掉的订阅重新打开"是同一个意图
    existing["enabled"] = True
    for field, value in clean.items():
        if value:  # 名字和备注只在非空时覆盖，别把之前记下的群名抹掉
            existing[field] = value
    existing["updated_at"] = _now_ms()
    return {"subscription": public_subscription(existing), "created": False}


@app.patch("/api/subscriptions/{sub_id}")
async def patch_subscription(
    sub_id: str, body: SubscriptionPatchBody, request: Request, user_id: str | None = Query(default=None)
):
    owner = _owner(request, user_id)
    row = _owned_subscription(sub_id, owner)
    payload = body.model_dump(exclude_none=True)
    if not payload:
        raise HTTPException(status_code=400, detail="没有要改的字段")
    if payload.get("enabled") is not None:
        row["enabled"] = bool(payload["enabled"])
    for field, label, limit in (
        ("note", "备注", _SUB_NOTE_MAX),
        ("group_name", "群名", _SUB_NAME_MAX),
        ("sender_name", "发送者备注", _SUB_NAME_MAX),
    ):
        if field in payload:
            row[field] = _clean_text(payload[field], field=label, limit=limit)
    row["updated_at"] = _now_ms()
    return {"subscription": public_subscription(row)}


@app.delete("/api/subscriptions/{sub_id}")
async def delete_subscription(sub_id: str, request: Request, user_id: str | None = Query(default=None)):
    owner = _owner(request, user_id)
    _owned_subscription(sub_id, owner)
    STATE.subscriptions.pop(sub_id, None)
    return {"deleted": True}


@app.get("/api/subscriptions/routing")
async def routing(
    request: Request,
    group_id: str = Query(...),
    sender_id: str | None = Query(default=None),
):
    """**投递名单**：这条消息要扇给谁。服务令牌专属。

    bot 每处理完一条消息就调它一次，拿到 user_id 列表，然后给每个人写一条
    自己的通知 —— "并集处理、按订阅扇出"里"扇给谁"这一步。

    `sender_id` 省略 = "这个群里**任何**发送者"。**两种语义不要混**：
    正常投递必须给 sender_id（否则就成了"订整个群"，而那正是被三层规则堵死的
    东西）；只有缺口告警用它 —— 缺口是**群级**事件（"这个群中间断了一段"），
    凡是订了这个群里任何人的用户都该知道。

    必须是服务令牌：一个普通用户拿着自己的令牌就能看到全局投递名单
    （谁订了哪个来源），那是别人的订阅关系。
    """
    _require_service(request)
    if STATE.fail_routing:
        # 故障注入：给 bot 一个"查不动名单"的场景。它必须把 raw 留在 pending，
        # 绝不能当成"没人要"。见 State.fail_routing 的说明。
        raise HTTPException(status_code=500, detail="注入的故障：投递名单查不动")
    rows = [
        r
        for r in STATE.subscriptions.values()
        if r["enabled"]
        and r["group_id"] == str(group_id)
        and (sender_id is None or r["sender_id"] == str(sender_id))
    ]
    # 去重：同一个人可以订同一个群里的多个人，缺口告警里必须只通知他一次
    return {"user_ids": sorted({r["user_id"] for r in rows})}


# ---------------------------------------------------------------------------
# 3b. 用户、注册、邀请码
#
# ⚠️ `/api/register` 是**鉴权之外**的唯一入口。它的安全完全由三样东西担着：
# 邀请码、QQ 验证码（bot 只发给能收到它消息的人）、以及猜错次数上限。
# ---------------------------------------------------------------------------


def _check_invite_usable(code: str | None) -> dict:
    """只检查，不消耗。"""
    normalized = (code or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="需要邀请码才能注册。请向服务提供者索取。")
    row = STATE.invites.get(normalized)
    if row is None:
        raise HTTPException(status_code=400, detail="邀请码无效")
    if row["expires_at"] is not None and int(row["expires_at"]) < _now_ms():
        raise HTTPException(status_code=400, detail="邀请码已过期")
    if int(row["used_count"]) >= int(row["max_uses"]):
        raise HTTPException(status_code=400, detail="邀请码已用完")
    return row


def _consume_invite(code: str | None) -> None:
    row = _check_invite_usable(code)
    row["used_count"] = int(row["used_count"]) + 1


def _verify_code_matches(qq: str, code: str) -> bool:
    """校验 QQ 验证码。**成功即作废**（一次一用），失败累计尝试次数，超限作废。

    任何异常路径都返回 False —— 这是认证入口，失败即拒绝。
    """
    row = STATE.verify_codes.get(qq)
    if row is None:
        return False
    if int(row["expires_at"]) < _now_ms():
        STATE.verify_codes.pop(qq, None)
        return False
    if int(row["attempts"]) >= VERIFY_MAX_ATTEMPTS:
        STATE.verify_codes.pop(qq, None)
        return False
    if not _same_secret(str(row["code"]), (code or "").strip()):
        row["attempts"] = int(row["attempts"]) + 1
        return False
    STATE.verify_codes.pop(qq, None)
    return True


@app.post("/api/verify/request")
async def request_verify_code(body: VerifyBody, request: Request):
    """给某个 QQ 签发验证码。**只允许服务令牌（bot）调。**

    否则任何人只要知道别人的 QQ 就能一直刷新他的验证码（拒绝服务），
    也把 6 位码的猜测窗口拉长。bot 拿到码之后**自己用 QQ 私聊发给对方** ——
    这是整条注册链路的信任基础。
    """
    _require_service(request)
    _note(request, body.model_dump())
    qq = str(body.qq or "").strip()
    if not QQ_RE.match(qq):
        raise HTTPException(
            status_code=400, detail="QQ 号看起来不对：应该是 5~12 位数字，且不以 0 开头"
        )
    code = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = _now_ms() + VERIFY_CODE_TTL_SECONDS * 1000
    STATE.verify_codes[qq] = {"qq": qq, "code": code, "expires_at": expires_at, "attempts": 0}
    logger.info("签发验证码 qq=%s（%d 秒内有效）", qq, VERIFY_CODE_TTL_SECONDS)
    return {"qq": qq, "code": code, "expires_at": expires_at, "ttl": VERIFY_CODE_TTL_SECONDS}


@app.post("/api/register")
async def register(body: RegisterBody, request: Request):
    """注册新用户，或给已有用户**轮换令牌**。**不需要任何令牌**（注册的前提就是还没有）。

    返回值里的 `token` 是明文令牌，只会出现这一次 —— bot/前端必须让用户当场
    存走。丢了可以用同样的流程再换一个（前提是 ALLOW_TOKEN_ROTATION）。
    """
    _note(request, body.model_dump())
    qq = str(body.qq or "").strip()
    if not QQ_RE.match(qq):
        raise HTTPException(
            status_code=400, detail="QQ 号看起来不对：应该是 5~12 位数字，且不以 0 开头"
        )

    existing_id = STATE.user_index.get(qq)
    # 已有用户再走一次 = 轮换令牌。关掉时要在**动验证码之前**就拒绝，
    # 免得白烧一个码。
    if existing_id is not None and not ALLOW_TOKEN_ROTATION:
        raise HTTPException(
            status_code=409,
            detail="这个 QQ 已经注册过了，而且本服务不允许自助轮换令牌。请联系服务提供者。",
        )
    # 邀请码只对**新用户**有意义；已有用户轮换令牌不需要（他已经是用户了）。
    # 先检查（不消耗），免得验证码对了却在最后一步失败。
    if existing_id is None and SIGNUPS_REQUIRE_INVITE:
        _check_invite_usable(body.invite_code)

    if not _verify_code_matches(qq, body.code):
        # 真后端对"验证码不对/过期/猜错超限"返回 401（users.py 的 UserError），
        # 不是 400 —— 401 在这里的含义是"你没证明得了这个 QQ 是你的"。
        raise HTTPException(
            status_code=401,
            detail="验证码不对或已过期。请在 QQ 上给机器人发一条消息重新获取。",
        )

    if existing_id is None and SIGNUPS_REQUIRE_INVITE:
        _consume_invite(body.invite_code)

    token = TOKEN_PREFIX + secrets.token_urlsafe(32)
    now = _now_ms()
    created = existing_id is None
    if created:
        user_id = "usr_" + _new_id()[13:]
        STATE.users[user_id] = {
            "id": user_id,
            "qq": qq,
            "display_name": (body.display_name or "").strip() or None,
            "token_hint": token[: len(TOKEN_PREFIX) + 6],
            "status": "active",
            "created_at": now,
            "last_seen_at": now,
        }
        STATE.user_index[qq] = user_id
        logger.info("新用户注册 user=%s qq=%s", user_id, qq)
    else:
        user_id = existing_id
        # 轮换：旧令牌立刻失效（真后端是覆盖 token_hash，效果一样）
        for old, uid in list(STATE.user_tokens.items()):
            if uid == user_id:
                STATE.user_tokens.pop(old, None)
        user = STATE.users[user_id]
        if user.get("status") != "active":
            raise HTTPException(status_code=403, detail="这个账号已被停用")
        user["token_hint"] = token[: len(TOKEN_PREFIX) + 6]
        user["last_seen_at"] = now
        logger.info("用户轮换了令牌 user=%s qq=%s", user_id, qq)

    STATE.user_tokens[token] = user_id
    return {
        "user": public_user(STATE.users[user_id]),
        "token": token,
        "created": created,
        "notice": (
            "这个令牌只会显示这一次，请立刻保存。它同时是你的登录凭证和调用凭证。"
            "丢了可以用同样的方式（QQ 找机器人要验证码）再换一个。"
        ),
    }


@app.get("/api/me")
async def whoami(request: Request):
    """我是谁。前端登录后第一件事就是调它 —— 令牌对不对一次就知道。"""
    identity = _identity(request)
    if identity.is_service:
        return {"scope": "service", "user": None}
    user = STATE.users.get(identity.user_id or "")
    if user is None:
        raise HTTPException(status_code=401, detail="令牌无效")
    return {"scope": "user", "user": public_user(user)}


@app.post("/api/invites")
async def create_invite(body: InviteBody, request: Request):
    """发一个邀请码。**只有服务令牌能发** —— 它就是"谁能注册"的开关。"""
    _require_service(request)
    _note(request, body.model_dump())
    code = "inv_" + _new_id()[13:]
    now = _now_ms()
    max_uses = max(1, int(body.max_uses or 1))
    row = {
        "code": code,
        "note": body.note,
        "max_uses": max_uses,
        "used_count": 0,
        "expires_at": (now + int(body.ttl_seconds) * 1000) if body.ttl_seconds else None,
        "created_at": now,
    }
    STATE.invites[code] = row
    logger.info("签发邀请码 code=%s note=%s max_uses=%s", code, body.note, max_uses)
    return dict(row)


@app.get("/api/invites")
async def list_invites(request: Request):
    _require_service(request)
    rows = sorted(STATE.invites.values(), key=lambda r: -(r.get("created_at") or 0))
    return {"invites": rows}


@app.get("/api/users")
async def list_users(request: Request):
    """所有用户。**不发令牌、不发摘要**，只是名单。"""
    _require_service(request)
    rows = sorted(STATE.users.values(), key=lambda r: -(r.get("created_at") or 0))
    return {"users": [public_user(r) for r in rows], "count": len(rows)}


@app.get("/api/users/lookup")
async def lookup_user(request: Request, qq: str = Query(...)):
    """按 QQ 号查用户。服务令牌专属。

    这是 bot 的**身份解析**入口：QQ 侧的一切身份锚点都是 QQ 号，而数据层的
    租户是 user_id。查不到返回 404 而不是空对象：调用方要能区分"这个人还没注册"
    和"后端没答上来"，前者提示他去注册，后者保持 pending 重试。
    """
    _require_service(request)
    user_id = STATE.user_index.get(str(qq).strip())
    if user_id is None:
        raise HTTPException(status_code=404, detail="这个 QQ 还没有注册")
    return {"user": public_user(STATE.users[user_id])}


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
    """自检用：看假后端到底收到了什么、按什么顺序。

    多用户之后这里多出的 `user_id` 是**自检专用**：契约的读投影里没有这个字段
    （调用方已经知道那是谁），但断言"这条通知是谁的"时必须看得到。
    """
    return {
        "sequence": [f"{c['method']} {c['path']}" for c in STATE.calls],
        "calls": STATE.calls,
        "messages": list(STATE.messages.values()),
        "patched_messages": STATE.patched_messages,
        "notifications": [
            {**_effective(n), "user_id": n.get("user_id")} for n in STATE.notifications.values()
        ],
        "corrections": STATE.correction_rows,
        "subscriptions": [dict(r) for r in STATE.subscriptions.values()],
        "users": [public_user(u) for u in STATE.users.values()],
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
        "stats": {f"{uid}/{day}": row for (uid, day), row in STATE.stats.items()},
        "digest_logs": STATE.digest_logs,
        "state_keys": [f"{uid}/{ns}/{key}" for (uid, ns, key) in STATE.kv],
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
    parser.add_argument(
        "--api-token", default="", help="服务令牌（bot 用的那个）；非空则校验 Authorization: Bearer"
    )
    parser.add_argument(
        "--media-max-bytes", type=int, default=DEFAULT_MEDIA_MAX_BYTES, help="附件大小上限"
    )
    parser.add_argument(
        "--signup-open",
        action="store_true",
        help="注册不需要邀请码（等价于真后端的 SIGNUP_MODE=open）",
    )
    parser.add_argument(
        "--no-token-rotation",
        action="store_true",
        help="已有用户不能自助轮换令牌（再注册一次返回 409）",
    )
    args = parser.parse_args(argv)

    global SIGNUPS_REQUIRE_INVITE, ALLOW_TOKEN_ROTATION
    STATE.fail_first = args.fail_first
    STATE.fail_attachments = args.fail_attachments
    STATE.token = args.api_token
    STATE.media_max_bytes = args.media_max_bytes
    SIGNUPS_REQUIRE_INVITE = not args.signup_open
    ALLOW_TOKEN_ROTATION = not args.no_token_rotation
    if args.fail_first:
        logger.info("已启用故障注入：前 %d 次 POST /api/messages 返回 500", args.fail_first)
    if args.api_token:
        logger.info("已启用令牌校验（这是服务令牌；用户令牌由 /api/register 签发）")
    if args.signup_open:
        logger.info("注册模式：open（不需要邀请码）")
    logger.info("假后端监听 http://%s:%d", args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
