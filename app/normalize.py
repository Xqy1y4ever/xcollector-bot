"""OneBot 事件 → 归一化消息。

这是 bot 的核心职责：**把 OneBot 协议的所有脏活干完**，让 backend 拿到的
就是「一条已经能直接读的消息」。具体包括：

  1. 消息段 → 纯文本（图片/文件/表情都以占位符呈现，不会变成空串）；
  2. 合并转发**递归展开**（NapCat 对合并转发的消息体是空的，只有 id；
     不展开这条通知就没了）；
  3. time（秒）→ ts（毫秒）；
  4. 群名 / 发送者昵称补齐（卡片名优先于昵称）；
  5. 附件裁剪成 {type, url, name, size} 四个字段。

刻意不做的事：
  - **不下载附件**。URL 有时效性，但这个仓库跑在 NapCat 的机器上，
    而"落地保存"是 backend 的职责（它才知道存哪儿、留多久）。
    bot 只透传 URL —— 这也是拆分之后两边最重要的分工线。
  - 不做任何白名单/有效性判断（群白名单在 backend）。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from .config import Settings, get_settings
from .onebot.segments import parse_message
from .utils import now_ms

logger = logging.getLogger(__name__)

FORWARD_FAIL_PLACEHOLDER = "[合并转发展开失败]"
FORWARD_DEPTH_PLACEHOLDER = "[合并转发层级超过上限，已省略]"

# 群名查询失败后多久才允许再试一次。
# 为什么要负缓存：群号配错 / 机器人不在群里时 get_group_info 每次都失败，
# 若每条消息都去问一次，就会被这个必然失败的调用拖慢整条链路。
GROUP_NAME_RETRY_SECONDS = 300

# 只透传这四个字段给 backend —— 多一个字段都是耦合
ATTACHMENT_FIELDS = ("type", "url", "name", "size")


class OneBotCaller(Protocol):
    """归一化只需要 hub 的这两个动作。

    用 Protocol 而不是直接依赖 OneBotHub：send_test 工具可以用一个假 hub
    在没有 NapCat 的情况下跑完整条归一化链路（见 app/tools/send_test.py）。
    """

    async def get_forward_msg(self, forward_id: str) -> list[dict]: ...

    async def get_group_info(self, group_id: str) -> dict: ...


# ---------------------------------------------------------------------------
# 事件分类（纯函数，方便离线测试）
# ---------------------------------------------------------------------------


def message_kind(event: dict) -> str | None:
    """判断这条事件该怎么处理，返回 'group' / 'private' / None（不处理）。

    - 非 message 事件（心跳、通知）→ None
    - 机器人自己发的消息 → None。必须挡掉，否则机器人回复用户的私聊内容
      会被 NapCat 当作 message_sent 再推回来，形成自我循环。
    - discuss（讨论组）暂不支持 → 原样返回，由调用方忽略
    """
    if event.get("post_type") not in ("message", "message_sent"):
        return None
    if str(event.get("self_id")) == str(event.get("user_id")):
        return None
    kind = event.get("message_type")
    return str(kind) if kind else None


# ---------------------------------------------------------------------------
# 合并转发展开
# ---------------------------------------------------------------------------


async def expand_forwards(
    hub: OneBotCaller,
    text: str,
    forward_ids: list[str],
    depth: int,
    seen: set[str],
    max_depth: int,
) -> str:
    """递归展开合并转发，返回展开后的文本。

    两个保护：
      - max_depth：防"转发套转发"指数级放大；
      - seen：同一个 forward id 只展开一次，防环形引用。

    展开失败**绝不丢消息**：降级成 [合并转发展开失败] 占位符继续往后走。
    NapCat 的 get_forward_msg 在反向 WS 模式下偶发超时，
    这时候宁可让 backend 收到一条"有文本但内容是占位符"的消息
    （用户能在库里看到"这里本来有个合并转发"），也不要静默少一条通知。
    """
    if not forward_ids:
        return text
    if depth >= max_depth:
        # 还有没展开的内容，但不再往下走了 —— 明确标注，
        # 否则下游会以为"占位符之后就是全文"，看不出来被截断过
        return "\n".join(p for p in (text, FORWARD_DEPTH_PLACEHOLDER) if p.strip())

    chunks: list[str] = [text] if text.strip() else []
    for fid in forward_ids:
        if fid in seen:
            continue
        seen.add(fid)
        try:
            nodes = await hub.get_forward_msg(fid)
        except Exception as exc:
            logger.warning("展开合并转发失败 id=%s: %s", fid, exc)
            chunks.append(FORWARD_FAIL_PLACEHOLDER)
            continue

        lines: list[str] = []
        for node in nodes or []:
            inner = parse_message(node.get("message") or node.get("content") or [])
            sender = (node.get("sender") or {}).get("nickname") or ""
            nested = await expand_forwards(
                hub, inner.text, inner.forwards, depth + 1, seen, max_depth
            )
            lines.append(f"  <{sender}> {nested}")
        if lines:
            chunks.append("[合并转发内容]\n" + "\n".join(lines))
    return "\n".join(c for c in chunks if c.strip())


# ---------------------------------------------------------------------------
# 群名缓存
# ---------------------------------------------------------------------------


class GroupRegistry:
    """bot 见过的群（收到过消息的）以及群名缓存。

    为什么要单独一个类：
      - /api/status 要告诉 backend「bot 到底看得见哪些群」，
        这是拆分之后最容易踩的坑（backend 再也看不到 OneBot 的群列表了）；
      - 群名只查一次就缓存，查询失败做 5 分钟负缓存。
    """

    def __init__(self, retry_seconds: float = GROUP_NAME_RETRY_SECONDS):
        self._names: dict[str, str] = {}
        self._last_msg: dict[str, int] = {}
        self._failed_at: dict[str, float] = {}
        self._retry_seconds = retry_seconds

    def touch(self, group_id: str, ts: int) -> None:
        """记录「这个群刚有消息」，即使群名还不知道也要先上账。"""
        gid = str(group_id)
        prev = self._last_msg.get(gid)
        self._last_msg[gid] = ts if prev is None else max(prev, ts)

    def set_name(self, group_id: str, name: str | None) -> None:
        if name:
            self._names[str(group_id)] = name

    def name(self, group_id: str) -> str | None:
        return self._names.get(str(group_id))

    async def resolve_name(self, hub: OneBotCaller, group_id: str) -> str | None:
        """拿到群名（有缓存走缓存，没缓存查一次）。查不到返回 None，不抛异常。"""
        gid = str(group_id)
        cached = self._names.get(gid)
        if cached:
            return cached

        failed_at = self._failed_at.get(gid)
        if failed_at is not None and time.monotonic() - failed_at < self._retry_seconds:
            return None

        try:
            info = await hub.get_group_info(gid)
        except Exception as exc:
            logger.debug("查询群信息失败 group=%s: %s", gid, exc)
            self._failed_at[gid] = time.monotonic()
            return None

        name = (info or {}).get("group_name")
        if name:
            self._names[gid] = str(name)
            self._failed_at.pop(gid, None)
            return str(name)

        self._failed_at[gid] = time.monotonic()
        return None

    def snapshot(self) -> list[dict]:
        """给 /api/status 用：见过的群，最近有消息的排前面。"""
        rows = [
            {
                "group_id": gid,
                "group_name": self._names.get(gid),
                "last_msg_ts": ts,
            }
            for gid, ts in self._last_msg.items()
        ]
        rows.sort(key=lambda r: r["last_msg_ts"] or 0, reverse=True)
        return rows


# ---------------------------------------------------------------------------
# 归一化
# ---------------------------------------------------------------------------


@dataclass
class NormalizedMessage:
    """推给 backend 的消息体（字段名即后端契约，别随手改）。"""

    source: str
    message_id: str
    group_id: str
    group_name: str | None
    sender_id: str
    sender_name: str
    ts: int
    text: str
    at_all: bool = False
    mentions: list[str] = field(default_factory=list)
    reply_to: str | None = None
    attachments: list[dict] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "message_id": self.message_id,
            "group_id": self.group_id,
            "group_name": self.group_name,
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "ts": self.ts,
            "text": self.text,
            "at_all": self.at_all,
            "mentions": self.mentions,
            "reply_to": self.reply_to,
            "attachments": self.attachments,
            # raw 只作留档：backend 不会再解析它，出问题时能人工比对
            "raw": self.raw,
        }


def attachments_payload(attachments: list[Any]) -> list[dict]:
    """裁剪附件字段。多余的 local_path / extracted_text 一律不带过去。"""
    out: list[dict] = []
    for att in attachments:
        out.append(
            {
                "type": getattr(att, "type", None),
                "url": getattr(att, "url", None),
                "name": getattr(att, "name", None),
                "size": getattr(att, "size", None),
            }
        )
    return out


class MessageNormalizer:
    def __init__(
        self, settings: Settings | None = None, groups: GroupRegistry | None = None
    ):
        self.settings = settings or get_settings()
        self.groups = groups or GroupRegistry()

    async def normalize(self, event: dict, hub: OneBotCaller) -> NormalizedMessage | None:
        """群消息事件 → NormalizedMessage。非群消息返回 None。"""
        if message_kind(event) != "group":
            return None

        group_id = str(event.get("group_id") or "")
        sender_id = str(event.get("user_id") or "")
        parsed = parse_message(
            event.get("message") or event.get("raw_message") or []
        )

        # time 缺失或为 0 时退化成"现在"。用 0 会让这条消息在库里排到 1970 年，
        # 比时间轻微不准糟糕得多。
        ts = int(event.get("time") or 0) * 1000 or now_ms()

        sender = event.get("sender") or {}
        # card（群名片）优先于 nickname：群通知里通常靠名片区分"班长/学委"，
        # 昵称往往是一串无意义的网名
        sender_name = str(
            sender.get("card") or sender.get("nickname") or sender_id
        )

        text = await expand_forwards(
            hub,
            parsed.text,
            parsed.forwards,
            0,
            set(),
            self.settings.forward_max_depth,
        )

        # 群名：先上账再解析。即使 get_group_info 失败，
        # 这个群也会出现在 /api/status 的 groups 里（group_name 为 null），
        # backend 就还能知道"消息确实进来了，只是名字没查到"。
        self.groups.touch(group_id, ts)
        group_name = await self.groups.resolve_name(hub, group_id)

        return NormalizedMessage(
            source="qq",
            message_id=str(event.get("message_id") or ""),
            group_id=group_id,
            group_name=group_name,
            sender_id=sender_id,
            sender_name=sender_name,
            ts=ts,
            text=text,
            at_all=parsed.at_all,
            mentions=list(parsed.mentions),
            reply_to=parsed.reply_id,
            attachments=attachments_payload(parsed.attachments),
            raw=event,
        )


def display_group(group_id: str, group_name: str | None) -> str:
    """日志里显示群：`NOVA官方通知群(673504310)` / `群673504310`。"""
    return f"{group_name}({group_id})" if group_name else f"群{group_id}"
