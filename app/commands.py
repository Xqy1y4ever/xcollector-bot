"""私聊指令路由。

设计原则：
  - **准入先于一切**：不在 COMMAND_WHITELIST 里的私聊消息直接忽略（只记 DEBUG）。
    不回复、不报错 —— 回复等于告诉陌生人"这里有个机器人"。
  - **解析是纯函数**：parse_command / is_yes / is_no / 各种 format_* 都不碰网络，
    所以 tests/check_commands.py 能在没有 NapCat、没有 backend 的机器上跑。
  - **状态放后端**：待确认的 /add 和 /list 的编号映射都通过
    `PUT/GET/DELETE /api/state/{namespace}/{user_id}` 存在后端（契约第 11 节）。
    bot 被 kill -9 再起来之后，用户回的那句 `y` 仍然找得到它对应的那条待确认 ——
    这是"bot 不持有需要跨重启存活的状态"这条规则在指令路径上的落地。
  - **解析在本地**：`/add` 的抽取（规则 + LLM）是 bot 的职责，后端不认识"通知"。
    契约里已经没有 `/api/tasks/manual` 了。

状态命名空间（建议取值来自契约）：
  command_pending —— 待确认的 /add，TTL = PENDING_TTL_SECONDS
  command_list    —— /list 的「编号 → 通知 id」映射，TTL = LIST_STATE_TTL_SECONDS
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Protocol

from .backend_client import BackendClient, BackendError, BackendRejected
from .config import Settings, get_settings
from .normalize import message_kind
from .onebot.segments import parse_message
from .pipeline.runner import MANUAL_GROUP_NAME, create_manual_notification, parse_content
from .utils import now_ms, truncate

logger = logging.getLogger(__name__)

# 发一条私聊消息的长度上限。QQ 单条消息的上限远大于此，但太长的回执
# 在手机上要滑半天；1500 这个值和 /api/send/* 的截断阈值保持一致。
MAX_REPLY_CHARS = 1500
TRUNCATE_SUFFIX = "…（已截断）"

BACKEND_DOWN_REPLY = "后端暂时不可用，请稍后再试"
UNKNOWN_COMMAND_REPLY = "未知指令，发 /help 查看可用指令"
NO_ARG_ADD_REPLY = "用法：/add <内容>\n例：/add 明天下午3点 交实验报告"
NO_ARG_DONE_REPLY = "先发 /list 看看有哪些待办，再用 /done <编号>。"
NO_ARG_DEL_REPLY = "先发 /list 看看有哪些待办，再用 /del <编号>。"

# 订阅指令的引导语。三条都写得很啰嗦，是刻意的：订阅参数（群号 / QQ 号）用户
# 记不住，写错了又**不会报错**、只会"什么都没有" —— 那是最难发现的故障。
SUBSCRIBE_USAGE = (
    "用法：\n"
    "/订阅 <群号> <发送者QQ> [备注]   ← 订某个群里某个人发的通知\n"
    "/订阅 <编号>                      ← 订 /来源 列表里的第 n 个\n"
    "不确定填什么就先发 /来源 看看。"
)
SUBSCRIBE_NEED_SENDER = (
    "还需要发送者的 QQ 号：订阅的最小单位是「某个群里**某个人**说的话」，"
    "不支持订阅整个群（那样群里任何人说话都会进你的清单）。\n"
    "用法：/订阅 <群号> <发送者QQ>，或先发 /来源 再 /订阅 <编号>。"
)
SOURCES_STALE_REPLY = "来源编号已过期或还没生成，请先发 /来源 刷新列表。"
SUBS_STALE_REPLY = "订阅编号已过期或还没生成，请先发 /订阅列表 刷新列表。"

# 一次最多列多少条来源 / 取多少条。企微式的长列表在 QQ 里没人看得完，
# 剩下的让用户用关键词缩小。
SOURCE_LIST_LIMIT = 200
SOURCE_REPLY_LIMIT = 15
# 编号映射过期（或 bot 重启后没重建）时的回执。**绝不能猜** ——
# 猜错的代价是把用户没打算动的那条通知标成完成。
STALE_LIST_REPLY = "编号列表已过期或还没生成，请先发 /list 刷新列表。"
NO_PENDING_REPLY = "没有待确认的添加（可能已超时），请重新发送 /add。"

PENDING_NS = "command_pending"
LIST_NS = "command_list"

# /list 一次向 backend 取多少条（取回来再按用户要的 n 截断）
LIST_FETCH_LIMIT = 50
LIST_DEFAULT_N = 10
LIST_MAX_N = 50

DONE_WORDS = {"y", "yes", "是", "确认", "好", "好的", "嗯"}
NO_WORDS = {"n", "no", "否", "取消", "不", "不用"}

# 全角/其它斜杠写法。手机中文输入法打出来的就是 ／，不兼容这一条的话
# 用户会觉得"机器人没反应"。
SLASH_ALIASES = "／∕⁄"

HELP_TEXT = (
    "🤖 可用指令：\n"
    "/add <内容>    添加任务，例：/add 明天下午3点 交实验报告\n"
    "/list [n]      查看待办（默认前 10 条）\n"
    "/done <编号>   标记完成\n"
    "/del <编号>    移除（这不是通知 / 加错了）\n"
    "/cancel        取消待确认的添加\n"
    "── 订阅（决定你收到哪些来源的通知）──\n"
    "/来源 [关键词] 看看有哪些可以订的来源\n"
    "/订阅 <群号> <发送者QQ> [备注]   订某个群里某个人发的通知\n"
    "/订阅 <编号>   订 /来源 列表里的第 n 个\n"
    "/订阅列表      看自己订了哪些\n"
    "/退订 <编号>   退掉一条\n"
    "── 账号 ──\n"
    "/注册          拿注册验证码（还没注册过的话先发这个）\n"
    "/help          显示这条帮助"
)

# 让用户能认出并复制的那一段提示。注册要在网页上完成，所以这里必须说清去哪。
REGISTER_HINT = "拿到验证码后，到网页上用「QQ 号 + 验证码 + 邀请码」完成注册。"

NOT_REGISTERED_REPLY = (
    "你还没有注册。发 /注册 拿一个验证码，"
    "然后到网页上用「QQ 号 + 验证码 + 邀请码」注册。"
)

# 服务端只发这一种形状的验证码；写死在这里是为了在码被中间层改坏时早点发现
VERIFY_CODE_DIGITS = 6

# 后端没给 expires_at 时用的兜底（正常路径用不上，只是别让用户看到"0 分钟"）
VERIFY_CODE_FALLBACK_MINUTES = 10

# 待确认的添加、/list 的编号映射之外的另外两份"编号纸"：
# /来源 的结果和 /订阅列表 的结果都要编号，用户才能用 /订阅 <编号> 和 /退订 <编号>。
SOURCES_NS = "command_sources"
SUBS_NS = "command_subs"

# 指令名 → 归一化后的名字。中文指令和英文别名都收，因为这是给中文用户用的。
# **只做指令名归一化，不做参数处理**。
COMMAND_ALIASES = {
    "注册": "register",
    "订阅": "subscribe",
    "订阅列表": "subscriptions",
    "退订": "unsubscribe",
    "来源": "sources",
}

# 不需要在本系统注册就能用的指令。注册本身当然是第一个 —— 否则谁都注册不了。
# （群里已经注册过的用户当然也能用，这只是不要求。）
NO_ACCOUNT_COMMANDS = {"register", "help"}

# 固定用 UTC+8 渲染时间，和 DIGEST_TZ 默认值保持一致。
# 之所以不做成可配项：机器人回复里的"09-17 15:00"是给人看的相对时间，
# 全系统统一一个时区才不会出现"digest 说 21:30、列表说 13:30"这种灵异现象。
_FALLBACK_TZ = timezone(timedelta(hours=8))


def local_tz() -> tzinfo:
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Asia/Shanghai")
    except Exception:
        return _FALLBACK_TZ


# ---------------------------------------------------------------------------
# 纯函数：解析
# ---------------------------------------------------------------------------


@dataclass
class ParsedCommand:
    name: str  # 小写后的指令名，不含前缀
    arg: str  # 第一个空白之后的所有内容（保留原始大小写与内部空格）
    raw: str  # 原始输入


def parse_command(text: str, prefix: str = "/") -> ParsedCommand | None:
    """把一条私聊文本解析成指令。不是指令就返回 None。

    容忍三件事（都是实测中真会遇到的）：
      1. 全角斜杠 ／（中文输入法默认就是它）；
      2. 前后空白 / 换行；
      3. 指令名大小写（/ADD == /add），但**参数原样保留** ——
         "交实验报告" 和 "交实验报告 " 对抽取是有差别的。
    """
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None

    body = raw
    if SLASH_ALIASES and body[0] in SLASH_ALIASES:
        body = prefix + body[1:]
    if prefix and not body.startswith(prefix):
        return None
    body = body[len(prefix) :] if prefix else body

    body = body.strip()
    if not body:
        return None

    # 用 split(None, 1) 而不是 partition(" ")：用户从中文输入法敲出来的是
    # 全角空格 U+3000，只按半角空格切会把 "/list　5" 整条当成指令名。
    parts = body.split(None, 1)
    head = parts[0]
    # 参数只去掉首尾空白；中间的空格是用户输入的一部分，不能动
    arg = parts[1].strip() if len(parts) > 1 else ""
    name = head.lower()
    # 中文指令归一化成英文名，后面所有分支只认英文，避免"两种写法走两条路"
    return ParsedCommand(name=COMMAND_ALIASES.get(name, name), arg=arg, raw=raw)


def is_yes(text: str) -> bool:
    return text.strip().lower() in DONE_WORDS


def is_no(text: str) -> bool:
    return text.strip().lower() in NO_WORDS


def parse_index(arg: str) -> int | None:
    """把 /done /del 的参数解析成正整数编号（1 起）。非法返回 None。"""
    m = re.fullmatch(r"\s*#?(\d{1,3})\s*", arg or "")
    if not m:
        return None
    value = int(m.group(1))
    return value if value >= 1 else None


def parse_list_n(arg: str) -> int:
    """/list [n] 的 n，缺省 10，越界收敛到 1..50。"""
    m = re.fullmatch(r"\s*(\d{1,3})\s*", arg or "")
    if not m:
        return LIST_DEFAULT_N
    return max(1, min(LIST_MAX_N, int(m.group(1))))


# ---------------------------------------------------------------------------
# 纯函数：排版（中文宽度对齐是刚需，不然列表看起来就是一团）
# ---------------------------------------------------------------------------


def display_width(text: str) -> int:
    """估算终端/聊天窗口里的显示宽度：中日韩全角字符算 2 列。"""
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return width


def pad_to(text: str, width: int) -> str:
    text = text or ""
    gap = width - display_width(text)
    return text + (" " * gap if gap > 0 else " ")


def one_line(text: str) -> str:
    """日志用：把换行压成空格，保证一条消息只占一行。"""
    return " ".join((text or "").split())


def preview(text: str, limit: int | None = None) -> str:
    settings = get_settings()
    size = settings.log_preview_chars if limit is None else limit
    return truncate(one_line(text), size, "…")


def format_due(
    ts_ms: int | None,
    due_text: str | None = None,
    confidence: float = 0.0,
    now: int | None = None,
) -> str:
    """人类可读的截止时间。

    - due_at 为空 → 退回 due_text 原文；都没有 → "待确认"（绝不显示空白）
    - 置信度 0.6~0.9 → 前面加 `~`，提示"这是猜的，可能差一点"
    - 跨年的绝对时间带上年份，否则"01-05"会让人以为是明年还是今年
    """
    if ts_ms is None:
        return (due_text or "").strip() or "待确认"

    moment = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).astimezone(
        local_tz()
    )
    baseline = datetime.fromtimestamp(
        (now if now is not None else now_ms()) / 1000, tz=timezone.utc
    ).astimezone(local_tz())
    stamp = (
        moment.strftime("%Y-%m-%d %H:%M")
        if moment.year != baseline.year
        else moment.strftime("%m-%d %H:%M")
    )
    uncertain = 0.6 <= float(confidence or 0.0) <= 0.9
    return f"~{stamp}" if uncertain else stamp


def format_due_with_source(
    ts_ms: int | None, due_text: str | None, confidence: float = 0.0
) -> str:
    """回执里用：`09-17 15:00（明天下午3点）`。原文和解析结果一起给，用户好核对。"""
    human = format_due(ts_ms, due_text, confidence)
    source = (due_text or "").strip()
    if ts_ms is not None and source and source != human:
        return f"{human}（{source}）"
    return human


def format_add_receipt(task: dict) -> str:
    title = (task.get("title") or "（未命名）").strip()
    due = format_due_with_source(
        task.get("due_at"), task.get("due_text"), task.get("due_confidence") or 0.0
    )
    location = (task.get("location") or "").strip() or "未提到"
    task_id = str(task.get("id") or "")
    lines = [
        f"✅ 已添加：{title}",
        f"🕒 截止：{due}",
        f"📍 地点：{location}",
    ]
    if task_id:
        lines.append(f"🔖 编号：#{task_id[-6:]}")
    return "\n".join(lines)


def format_confirm_prompt(preview_data: dict) -> str:
    """needs_confirm 时的追问文案。

    这里必须把"我理解成了什么"回显给用户 —— 抽取出错时，
    用户看到标题不对就能直接 n 掉，而不是先建出一条错任务再去删。
    """
    title = (preview_data.get("title") or "（没看懂）").strip()
    due = format_due_with_source(
        preview_data.get("due_at"),
        preview_data.get("due_text"),
        preview_data.get("due_confidence") or 0.0,
    )
    if preview_data.get("due_at") is None:
        due = "未识别"
    lines = [
        "⚠️ 我没解析出明确的截止时间。",
        f"我理解为：{title}",
        f"截止：{due}",
    ]
    location = (preview_data.get("location") or "").strip()
    if location:
        lines.append(f"地点：{location}")
    lines.append("仍然添加吗？回复 y 确认，n 取消。")
    return "\n".join(lines)


def format_list_reply(
    items: list[dict],
    *,
    total: int,
    shown: int | None = None,
    prefix: str = "/",
) -> str:
    """待办列表。items 已经是被显示的那一批（按 backend 返回的顺序）。"""
    shown = len(items) if shown is None else shown
    if not items:
        return "📋 目前没有待办。"

    rows: list[tuple[str, str, str]] = []
    for item in items:
        index = len(rows) + 1
        title = one_line(item.get("title") or "（未命名）")
        due = format_due(
            item.get("due_at"), item.get("due_text"), item.get("due_confidence") or 0.0
        )
        location = one_line(item.get("location") or "")
        rows.append((f"{index}. {title}", due, location))

    title_width = min(22, max(display_width(r[0]) for r in rows))
    due_width = max(display_width(r[1]) for r in rows)

    lines = [f"📋 待办 {total} 条（显示前 {shown}）"]
    for head, due, location in rows:
        line = pad_to(head, title_width) + pad_to(due, due_width)
        if location:
            line += "  " + location
        lines.append(line.rstrip())
    lines.append(f"用 {prefix}done <编号> 标记完成，{prefix}del <编号> 移除")
    return truncate("\n".join(lines), MAX_REPLY_CHARS, TRUNCATE_SUFFIX)


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------


@dataclass
class PendingAdd:
    """一条等着用户回 y/n 的 /add。

    存的是**原始文本 + 那次解析的结果**：确认时直接用同一份结果建条，
    保证"用户看到的预览"和"最终建出来的任务"出自同一次解析。
    这份状态活在**后端**的 bot_state 里（契约第 11 节），所以 bot 重启不影响它。
    """

    text: str
    created_at: float  # 墙钟秒（存进后端时是 created_at_ms / 1000）
    preview: dict
    result: dict | None = None


def pending_expired(
    pending: PendingAdd, ttl_seconds: float, now: float | None = None
) -> bool:
    """待确认状态是否过期（纯函数，方便离线测试）。

    只看"现在 - 创建时间 > TTL"，不关心单位是秒还是毫秒。
    后端那边也会按 ttl_seconds 过期，这里是双保险：万一后端没清，
    bot 也不能拿一份很旧的待确认去建条。
    """
    return ((time.monotonic() if now is None else now) - pending.created_at) > ttl_seconds


@dataclass
class ListedItem:
    """一次 /list 里的一个编号。用户下次 /done 3 就是找它。"""

    notif_id: str
    title: str


class PrivateSender(Protocol):
    async def send_private_msg(self, user_id: str, message: str) -> dict: ...


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------


class CommandRouter:
    def __init__(
        self,
        backend: BackendClient,
        sender: PrivateSender,
        settings: Settings | None = None,
    ):
        self.backend = backend
        self.sender = sender
        self.settings = settings or get_settings()

    # ---------------- 入口 ----------------

    async def _resolve_owner(self, qq: str) -> str | None:
        """QQ 号 → 数据归属（`usr_...`）。查不到返回 None。

        多用户之后这两个 id 是**两件事**，绝不能混：QQ 号是身份锚点（谁在说话），
        `user_id` 是租户（数据属于谁）。用 QQ 号当 user_id 会串数据。

        查不到（404）和查不动（后端挂了）必须分开：前者是"这个人还没注册"，
        要提示他去注册；后者是暂时故障，报错让他重试。`get_user_by_qq`
        把 404 翻译成 None，其余错误照抛。
        """
        try:
            user = await self.backend.get_user_by_qq(qq)
        except BackendError:
            raise
        if not user:
            return None
        return str(user.get("id") or "") or None

    async def handle_private_event(self, event: dict) -> None:
        """处理一条私聊事件。非私聊直接返回。"""
        if message_kind(event) != "private":
            return

        qq = str(event.get("user_id") or "")
        user_name = self.settings.command_whitelist_map.get(qq, qq)
        parsed_msg = parse_message(event.get("message") or event.get("raw_message") or [])
        text = (parsed_msg.text or "").strip()
        if not text:
            return

        command = parse_command(text, self.settings.command_prefix)

        # 白名单的判定放在**知道这是什么指令之后**：`/注册` 必须对任何能找到
        # 机器人的人开放 —— 注册的前提就是"这个人还不在名单里"。别的指令照旧
        # 只看白名单，所以这不等于把机器人打开。
        registerish = command is not None and command.name in NO_ACCOUNT_COMMANDS
        if not registerish and not self.settings.in_command_whitelist(qq):
            # 只记 DEBUG：这类消息可能来自任意陌生人，INFO 级别会被刷爆，
            # 而且我们也不想在日志里留下"机器人回复过谁"的痕迹
            logger.debug("忽略非白名单私聊 qq=%s", qq)
            return

        try:
            if command is None:
                if not self.settings.in_command_whitelist(qq):
                    return
                await self._maybe_confirm(qq, user_name, text)
                return
            await self._dispatch(command, qq, user_name)
        except BackendRejected as exc:
            # 后端明确拒绝（参数不对 / 对象不存在）：把原因说清楚，别甩"稍后再试"
            await self._reply(qq, f"⚠️ {reject_text(exc)}")
            log_command(
                command=command.name if command else "-",
                user_id=qq,
                user_name=user_name,
                raw=text,
                result="error",
                note=str(exc),
            )
        except BackendError as exc:
            await self._reply(qq, BACKEND_DOWN_REPLY)
            log_command(
                command=command.name if command else "-",
                user_id=qq,
                user_name=user_name,
                raw=text,
                result="error",
                note=str(exc),
            )
        except Exception as exc:  # 兜底：任何意外都要让用户知道"这条没成"
            logger.exception("指令处理失败: %s", exc)
            await self._reply(qq, "⚠️ 处理这条指令时出错了，请稍后再试。")
            log_command(
                command=command.name if command else "-",
                user_id=qq,
                user_name=user_name,
                raw=text,
                result="error",
                note=f"{type(exc).__name__}: {exc}",
            )

    async def _dispatch(self, command: ParsedCommand, qq: str, user_name: str) -> None:
        name = command.name

        # `/help` 和 `/注册` 不需要账号
        if name == "help":
            await self._reply(qq, HELP_TEXT)
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="help",
            )
            return
        if name == "register":
            await self._handle_register(qq, user_name, command)
            return

        # 其余全部要有账号。解析失败要**说清楚是为什么** —— 静默不回复会让
        # 用户以为机器人坏了，反复发同一条指令。
        owner = await self._resolve_owner(qq)
        if owner is None:
            await self._reply(qq, NOT_REGISTERED_REPLY)
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="QQ 未注册",
            )
            return

        if name == "add":
            await self._handle_add(command, qq, owner, user_name)
        elif name == "list":
            await self._handle_list(command, qq, owner, user_name)
        elif name in ("done", "del"):
            await self._handle_mark(command, qq, owner, user_name, name)
        elif name == "subscribe":
            await self._handle_subscribe(command, qq, owner, user_name)
        elif name == "subscriptions":
            await self._handle_subscriptions(command, qq, owner, user_name)
        elif name == "sources":
            await self._handle_sources(command, qq, owner, user_name)
        elif name == "unsubscribe":
            await self._handle_unsubscribe(command, qq, owner, user_name)
        elif name == "cancel":
            await self._clear_pending(qq, owner)
            await self._reply(qq, "已取消。")
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="cancelled",
            )
        else:
            await self._reply(qq, UNKNOWN_COMMAND_REPLY)
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="unknown",
            )

    # ---------------- 后端里的指令状态 ----------------
    #
    # 这一组都收两个参数，别混：
    #   qq    —— QQ 号。**key**（哪个人），也是回复的对象。
    #   owner —— `usr_...` 数据归属。**租户**，后端按它隔离。
    #
    # key 之所以继续用 QQ 号而不是 owner：QQ 号是稳定的身份锚点，
    # 用户轮换令牌时 owner 不变、QQ 也不变，两者都可以；但日志和人工排查
    # 都是按 QQ 号认人的，key 用 QQ 号能直接对上。
    # 关键是 `put_state/get_state` 必须把 owner 传给后端 —— 那才是隔离的依据。

    async def _save_pending(
        self, qq: str, owner: str, text: str, preview_data: dict, result: dict | None
    ) -> None:
        await self.backend.put_state(
            PENDING_NS,
            qq,
            {
                "text": text,
                "preview": preview_data,
                "result": result,
                "created_at_ms": now_ms(),
            },
            user_id=owner,
            ttl_seconds=self.settings.pending_ttl_seconds,
        )

    async def _load_pending(self, qq: str, owner: str) -> PendingAdd | None:
        value = await self.backend.get_state(PENDING_NS, qq, user_id=owner)
        if not isinstance(value, dict) or not value.get("text"):
            return None
        try:
            created_at = float(value.get("created_at_ms") or 0) / 1000.0
        except (TypeError, ValueError):
            created_at = 0.0
        pending = PendingAdd(
            text=str(value.get("text") or ""),
            created_at=created_at,
            preview=dict(value.get("preview") or {}),
            result=value.get("result") if isinstance(value.get("result"), dict) else None,
        )
        if pending_expired(pending, self.settings.pending_ttl_seconds, now=time.time()):
            await self._clear_pending(qq, owner)
            return None
        return pending

    async def _clear_pending(self, qq: str, owner: str) -> None:
        try:
            await self.backend.delete_state(PENDING_NS, qq, user_id=owner)
        except BackendError as exc:
            logger.warning("清除待确认状态失败 qq=%s: %s", qq, exc)

    async def _save_listing(
        self, qq: str, owner: str, namespace: str, items: list[dict]
    ) -> None:
        """把一份"编号 → 对象"的映射存进后端。

        `/list`、`/来源`、`/订阅列表` 都用它：用户回 `/done 3`、`/订阅 2`、
        `/退订 1` 时，bot 必须知道"3"指的是哪一条，而这个映射得扛住重启。
        """
        try:
            await self.backend.put_state(
                namespace,
                qq,
                {"items": items, "created_at_ms": now_ms()},
                user_id=owner,
                ttl_seconds=self.settings.list_state_ttl_seconds,
            )
        except BackendError as exc:
            # 列表本身已经发出去了，映射没存上只会让下一条指令要求刷新一次，
            # 比"整条指令报后端不可用"更轻
            logger.warning("保存编号映射失败 qq=%s ns=%s: %s", qq, namespace, exc)

    async def _load_numbered(
        self, qq: str, owner: str, namespace: str
    ) -> list[dict] | None:
        """读回一份编号映射；过期或没有都返回 None。"""
        value = await self.backend.get_state(namespace, qq, user_id=owner)
        if not isinstance(value, dict):
            return None
        raw_items = value.get("items")
        if not isinstance(raw_items, list) or not raw_items:
            return None
        try:
            created_at = float(value.get("created_at_ms") or 0) / 1000.0
        except (TypeError, ValueError):
            created_at = 0.0
        # 后端 TTL 是主保险，这里再用同一套 pending_expired 判一次：
        # 万一后端没清掉，bot 也不能拿一份很旧的编号列表去改条目。
        stale = PendingAdd("", created_at, {})
        if pending_expired(stale, self.settings.list_state_ttl_seconds, now=time.time()):
            try:
                await self.backend.delete_state(namespace, qq, user_id=owner)
            except BackendError:
                pass
            return None
        items = [it for it in raw_items if isinstance(it, dict)]
        return items or None

    async def _save_listing_items(
        self, qq: str, owner: str, items: list[ListedItem]
    ) -> None:
        await self._save_listing(
            qq,
            owner,
            LIST_NS,
            [{"notif_id": i.notif_id, "title": i.title} for i in items],
        )

    async def _load_listing(self, qq: str, owner: str) -> list[ListedItem] | None:
        raw_items = await self._load_numbered(qq, owner, LIST_NS)
        if not raw_items:
            return None
        out: list[ListedItem] = []
        for item in raw_items:
            if not item.get("notif_id"):
                continue
            out.append(
                ListedItem(notif_id=str(item["notif_id"]), title=one_line(item.get("title") or ""))
            )
        return out or None

    # ---------------- /add ----------------

    def _synthetic_raw(self, text: str, user_id: str, user_name: str) -> dict:
        """把手动输入包装成和群消息同构的 raw，好让抽取层完全复用。"""
        stamp = now_ms()
        return {
            "message_id": f"manual-{stamp}",
            "group_id": f"manual:{user_id}",
            "group_name": MANUAL_GROUP_NAME,
            "sender_id": user_id,
            "sender_name": user_name,
            "ts": stamp,
            "content": text,
            "text": text,
            "attachments": [],
            "at_all": False,
        }

    @staticmethod
    def _preview(result: dict | None, text: str) -> dict:
        if result is None:
            return {
                "title": text[:60],
                "summary": None,
                "location": None,
                "due_at": None,
                "due_text": None,
                "due_confidence": 0.0,
            }
        return {
            "title": result.get("title") or text[:60],
            "summary": result.get("summary"),
            "location": result.get("location"),
            "due_at": result.get("due_at"),
            "due_text": result.get("due_text"),
            "due_confidence": float(result.get("due_confidence") or 0.0),
        }

    def _confirm_reason(self, result: dict | None, degraded: bool) -> str | None:
        """要不要先回问。判定刻意保守：多问一句的成本远低于建错一条任务。"""
        if result is None:
            return "没能识别出这是一条任务"
        if result.get("due_at") is None:
            return "没能解析出明确的截止时间"
        if float(result.get("due_confidence") or 0.0) < self.settings.low_confidence_threshold:
            return "截止时间的把握不大"
        if degraded:
            return "模型不可用，这次只用规则解析"
        return None

    async def _handle_add(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        text = command.arg.strip()
        if not text:
            await self._reply(qq, NO_ARG_ADD_REPLY)
            log_command(
                command="add", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="缺少内容",
            )
            return

        # 抽取在 bot 本地做（规则 + LLM）—— 后端不认识"通知"
        result, degraded, _tokens = await parse_content(
            self._synthetic_raw(text, qq, user_name), self.settings
        )
        preview_data = self._preview(result, text)
        reason = self._confirm_reason(result, degraded)

        if reason:
            # 待确认状态存后端：bot 重启后用户那句 `y` 仍然有效
            await self._save_pending(qq, owner, text, preview_data, result)
            await self._reply(qq, format_confirm_prompt(preview_data))
            log_command(
                command="add", user_id=qq, user_name=user_name,
                raw=command.raw, result="needs_confirm",
                due=format_due(
                    preview_data.get("due_at"),
                    preview_data.get("due_text"),
                    preview_data.get("due_confidence") or 0.0,
                ),
                note=reason,
            )
            return

        created = await create_manual_notification(
            user_id=owner,
            text=text,
            sender_id=qq,
            sender_name=user_name,
            result=result,
            backend=self.backend,
            confirmed=False,
        )
        await self._reply(qq, format_add_receipt(created))
        log_command(
            command="add", user_id=qq, user_name=user_name,
            raw=command.raw, result="created",
            task_id=str(created.get("id") or ""),
            due=format_due(
                created.get("due_at"), created.get("due_text"),
                created.get("due_confidence") or 0.0,
            ),
        )

    async def _maybe_confirm(self, qq: str, user_name: str, text: str) -> None:
        """处理"不是指令"的私聊：可能是对 /add 的 y/n 回答，也可能什么都不是。

        为什么 y/n 要走这条分支而不是做成指令：用户被问了"仍然添加吗"之后，
        打的就是一个 y，让他再打 "/y" 是反人性的。

        只有 y/n 这类词才去后端查待确认状态 —— 别的闲聊不该产生任何请求。
        """
        if not (is_yes(text) or is_no(text)):
            logger.debug("忽略无法识别的私聊 qq=%s text=%s", qq, preview(text))
            return

        # 要知道这条待确认是谁的，必须先定出归属；没注册的人不可能有待确认
        owner = await self._resolve_owner(qq)
        if owner is None:
            return
        pending = await self._load_pending(qq, owner)
        if pending is None:
            logger.debug("没有待确认的添加 qq=%s text=%s", qq, preview(text))
            return
        await self._confirm(qq, owner, user_name, text, pending, yes=is_yes(text))

    async def _confirm(
        self,
        qq: str,
        owner: str,
        user_name: str,
        text: str,
        pending: PendingAdd,
        *,
        yes: bool,
    ) -> None:
        await self._clear_pending(qq, owner)
        if not yes:
            await self._reply(qq, "已取消。")
            log_command(
                command="add", user_id=qq, user_name=user_name,
                raw=text, result="cancelled", note="用户放弃待确认的添加",
            )
            return

        # 用用户当初看到的那次解析结果建条，保证回执和预览一致
        result = pending.result
        if result is None:
            result = {
                "title": pending.text[:60],
                "summary": pending.text,
                "evidence": pending.text[:200],
                "due_at": None,
                "due_text": None,
                "due_confidence": 0.0,
                "extractor": "manual",
                "conflict": False,
                "candidates": [],
            }

        created = await create_manual_notification(
            user_id=owner,
            text=pending.text,
            sender_id=qq,
            sender_name=user_name,
            result=result,
            backend=self.backend,
            confirmed=True,
        )
        await self._reply(qq, format_add_receipt(created))
        log_command(
            command="add", user_id=qq, user_name=user_name,
            raw=text, result="confirmed", task_id=str(created.get("id") or ""),
            due=format_due(
                created.get("due_at"), created.get("due_text"),
                created.get("due_confidence") or 0.0,
            ),
        )

    # ---------------- /list ----------------

    async def _handle_list(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        n = parse_list_n(command.arg)
        items = await self.backend.list_notifications(
            user_id=owner, status="active", limit=LIST_FETCH_LIMIT
        )
        try:
            total = await self.backend.count_notifications(user_id=owner, status="active")
        except BackendError:
            # 数不出来就用这一页的长度，回执上少一点准确性，但不会卡住用户
            total = len(items)
        total = max(total, len(items))
        selected = items[:n]

        # 编号→通知 id 的映射**按用户、存后端**：同一个群里两个人都发 /list，
        # 各自的"3 号"必须是各自看到的那条；映射还要能扛住 bot 重启。
        await self._save_listing_items(
            qq,
            owner,
            [
                ListedItem(notif_id=str(it.get("id")), title=one_line(it.get("title") or ""))
                for it in selected
            ],
        )
        await self._reply(
            qq,
            format_list_reply(
                selected, total=total, shown=len(selected), prefix=self.settings.command_prefix
            ),
        )
        log_command(
            command="list", user_id=qq, user_name=user_name,
            raw=command.raw, result="listed", note=f"共 {total} 条，显示 {len(selected)}",
        )

    # ---------------- /done /del ----------------

    async def _handle_mark(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str, name: str
    ) -> None:
        label = "done" if name == "done" else "archived"
        if not command.arg:
            await self._reply(
                qq, NO_ARG_DONE_REPLY if name == "done" else NO_ARG_DEL_REPLY
            )
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="缺少编号",
            )
            return

        listing = await self._load_listing(qq, owner)
        if not listing:
            # 过期的编号**绝不猜**：宁可让用户重发一次 /list
            await self._reply(qq, STALE_LIST_REPLY)
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="编号映射缺失或已过期",
            )
            return

        index = parse_index(command.arg)
        if index is None or index > len(listing):
            hint = f"编号要在 1~{len(listing)} 之间。"
            await self._reply(qq, f"⚠️ 编号无效。{hint}")
            log_command(
                command=name, user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="编号无效",
            )
            return

        item = listing[index - 1]
        # user_id 是租户，actor 是"谁改的"（界面上显示的就是它）——
        # 这两个字段在多用户改造里被刻意分开了，别写回成一个。
        await self.backend.correct_notification(
            item.notif_id,
            field="status",
            value=label,
            actor=f"qq:{qq}",
            user_id=owner,
        )
        if name == "done":
            await self._reply(qq, f"✅ 已完成：{item.title or item.notif_id[-6:]}")
        else:
            await self._reply(qq, f"🗑 已移除：{item.title or item.notif_id[-6:]}")
        log_command(
            command=name, user_id=qq, user_name=user_name,
            raw=command.raw, result=label, task_id=item.notif_id,
        )

    # ---------------- /注册 ----------------

    async def _handle_register(
        self, qq: str, user_name: str, command: ParsedCommand
    ) -> None:
        """签一个验证码并**私聊回给本人**。

        这是整条注册链路的信任基础：只有能收到这条消息的人才证明得了自己拥有
        这个 QQ 号。所以码不能回在群里（别人也能看到），只能私聊。

        `/注册` 对任何能找到机器人的人开放 —— 注册的前提就是"这个人还不在
        白名单里"。签发本身不写任何用户数据，拿不到码就注册不了。
        """
        issued = await self.backend.request_verify_code(qq)
        code = str(issued.get("code") or "")
        if not code:
            # 后端没给码：宁可让用户重试，也不能回一句没有码的"成功"
            logger.error("后端签发的验证码为空 qq=%s", qq)
            await self._reply(qq, "⚠️ 没能拿到验证码，请稍后再试。")
            log_command(
                command="register", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="后端没返回验证码",
            )
            return

        # 有效期直接由后端返回的 expires_at 推出来，**不在这里再配一个 TTL**：
        # 配两处就会出现"机器人说 10 分钟、后端其实 5 分钟"这种谁都没错的错。
        minutes = VERIFY_CODE_FALLBACK_MINUTES
        try:
            expires_ms = int(issued.get("expires_at") or 0)
        except (TypeError, ValueError):
            expires_ms = 0
        if expires_ms > 0:
            minutes = max(1, round((expires_ms - now_ms()) / 60000))

        await self._reply(
            qq,
            f"🔑 你的注册验证码：{code}\n{minutes} 分钟内有效，只能用于 QQ {qq}。\n"
            f"{REGISTER_HINT}\n（这条消息只发给你，别转发给别人。）",
        )
        log_command(
            command="register", user_id=qq, user_name=user_name,
            raw=command.raw, result="verify_code_sent",
            note=f"码长 {len(code)}",
        )

    # ---------------- 订阅 ----------------

    async def _handle_subscribe(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        """`/订阅 <群号> <发送者QQ> [备注]` 或 `/订阅 <编号>`（用 /来源 的编号）。

        **必须给出发送者**：订阅的最小单位是「某个群里某个人说的话」，
        没有"订整个群"这个选项。用户要是只给一个群号，这里要明确告诉他
        还缺什么，而不是报一句看不懂的错。
        """
        arg = command.arg.strip()
        if not arg:
            await self._reply(qq, SUBSCRIBE_USAGE)
            log_command(
                command="subscribe", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="缺少参数",
            )
            return

        parts = arg.split()
        group_id = sender_id = ""
        group_name = sender_name = None

        if len(parts) == 1 and parts[0].isdigit() and len(parts[0]) <= 3:
            # 一个短数字 = 用最近一次 /来源 的编号
            picked = await self._pick_source(qq, owner, parts[0])
            if picked is None:
                await self._reply(qq, SOURCES_STALE_REPLY)
                log_command(
                    command="subscribe", user_id=qq, user_name=user_name,
                    raw=command.raw, result="error", note="来源编号无效",
                )
                return
            group_id = str(picked.get("group_id") or "")
            sender_id = str(picked.get("sender_id") or "")
            group_name = picked.get("group_name")
            sender_name = picked.get("sender_name")
        elif len(parts) >= 2:
            group_id, sender_id = parts[0], parts[1]
        else:
            # 只给了一个群号 —— 这**正是**"订整个群"的写法，要说清楚为什么不行
            await self._reply(qq, SUBSCRIBE_NEED_SENDER)
            log_command(
                command="subscribe", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="只给了群号",
            )
            return

        note = " ".join(parts[2:]).strip() if len(parts) > 2 else ""
        body = await self.backend.add_subscription(
            owner,
            group_id=group_id,
            sender_id=sender_id,
            group_name=group_name,
            sender_name=sender_name,
            note=note or None,
        )
        sub = body.get("subscription") or {}
        created = bool(body.get("created"))
        verb = "已订阅" if created else "已经在订（帮你重新打开了）"
        await self._reply(
            qq,
            f"✅ {verb}：{source_label(sub)}",
        )
        log_command(
            command="subscribe", user_id=qq, user_name=user_name,
            raw=command.raw, result="created" if created else "exists",
            note=source_label(sub),
        )

    async def _pick_source(self, qq: str, owner: str, index_text: str) -> dict | None:
        """按 /来源 的编号取一个来源。编号过期就返回 None（**绝不猜**）。"""
        items = await self._load_numbered(qq, owner, SOURCES_NS)
        if not items:
            return None
        index = parse_index(index_text)
        if index is None or index > len(items):
            return None
        return items[index - 1]

    async def _handle_sources(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        """列出可以订的来源（这套部署见过的 (群, 发送者)）。

        新用户手上是空的 —— 不知道群号、也没有通知，所以必须有一份目录能挑。
        编号会存进后端，紧接着的 `/订阅 <编号>` 就有东西可指。
        """
        keyword = command.arg.strip() or None
        sources = await self.backend.list_sources(keyword=keyword, limit=SOURCE_LIST_LIMIT)
        if not sources:
            hint = "（换个关键词试试，或者让管理员确认机器人能看见那个群）"
            await self._reply(qq, f"没有找到可订阅的来源。{hint}")
            log_command(
                command="sources", user_id=qq, user_name=user_name,
                raw=command.raw, result="empty", note=keyword or "-",
            )
            return

        await self._save_listing(qq, owner, SOURCES_NS, sources)
        lines = [f"📚 可订阅的来源（共 {len(sources)} 条，发 /订阅 <编号> 即可订）："]
        for i, src in enumerate(sources[:SOURCE_REPLY_LIMIT], start=1):
            lines.append(f"{i}. {source_label(src)}")
        if len(sources) > SOURCE_REPLY_LIMIT:
            lines.append(f"…还有 {len(sources) - SOURCE_REPLY_LIMIT} 条，用 /来源 <关键词> 缩小范围")
        await self._reply(qq, "\n".join(lines))
        log_command(
            command="sources", user_id=qq, user_name=user_name,
            raw=command.raw, result="listed",
            note=f"{len(sources)} 条" + (f"，关键词={keyword}" if keyword else ""),
        )

    async def _handle_subscriptions(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        subs = await self.backend.list_subscriptions(owner)
        if not subs:
            await self._reply(qq, "你还没有订阅任何来源。发 /来源 看看有什么可以订。")
            log_command(
                command="subscriptions", user_id=qq, user_name=user_name,
                raw=command.raw, result="empty",
            )
            return
        await self._save_listing(qq, owner, SUBS_NS, subs)
        lines = [f"🔔 你的订阅（共 {len(subs)} 条，发 /退订 <编号> 可以退）："]
        for i, sub in enumerate(subs, start=1):
            mark = "" if sub.get("enabled") else "（已停用）"
            lines.append(f"{i}. {source_label(sub)}{mark}")
        await self._reply(qq, "\n".join(lines))
        log_command(
            command="subscriptions", user_id=qq, user_name=user_name,
            raw=command.raw, result="listed", note=f"{len(subs)} 条",
        )

    async def _handle_unsubscribe(
        self, command: ParsedCommand, qq: str, owner: str, user_name: str
    ) -> None:
        if not command.arg:
            await self._reply(qq, "用法：先发 /订阅列表，再发 /退订 <编号>")
            log_command(
                command="unsubscribe", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="缺少编号",
            )
            return

        subs = await self._load_numbered(qq, owner, SUBS_NS)
        index = parse_index(command.arg)
        if not subs or index is None or index > len(subs):
            # 编号过期就**绝不猜**：猜错的代价是退掉一条用户还想留的订阅，
            # 而"没收到通知"要过很久才会被发现
            await self._reply(qq, SUBS_STALE_REPLY)
            log_command(
                command="unsubscribe", user_id=qq, user_name=user_name,
                raw=command.raw, result="error", note="订阅编号无效或已过期",
            )
            return

        target = subs[index - 1]
        sub_id = str(target.get("id") or "")
        await self.backend.delete_subscription(owner, sub_id)
        await self._reply(qq, f"🗑 已退订：{source_label(target)}")
        log_command(
            command="unsubscribe", user_id=qq, user_name=user_name,
            raw=command.raw, result="deleted", note=source_label(target),
        )

    # ---------------- 回复 ----------------

    async def _reply(self, qq: str, text: str) -> None:
        payload = truncate(text, MAX_REPLY_CHARS, TRUNCATE_SUFFIX)
        try:
            await self.sender.send_private_msg(qq, payload)
        except Exception as exc:
            # 发不出去只说明 NapCat 掉了；指令本身的结果已经记在日志里了
            logger.warning("回复私聊失败 qq=%s: %s", qq, exc)


def source_label(item: Mapping[str, Any]) -> str:
    """"群名(群号) · 发送者名(QQ)" —— 给人看的来源标签。

    名字可能没有（老数据 / bot 没拿到群名片），那就只显示号码，
    而不是显示一个空括号。
    """
    group = str(item.get("group_name") or "").strip()
    gid = str(item.get("group_id") or "").strip()
    sender = str(item.get("sender_name") or "").strip()
    sid = str(item.get("sender_id") or "").strip()
    left = f"{group}({gid})" if group else gid
    right = f"{sender}({sid})" if sender else sid
    return f"{left} · {right}" if right else left


def reject_text(exc: BackendRejected) -> str:
    """把 `HTTP 400: <detail>` 还原成 `<detail>`。

    后端的 detail 是写给用户看的（"不支持订阅整个群…"），而 "HTTP 400: "
    这个前缀对用户没有意义 —— 直接展示会让人以为是机器人出了故障。
    """
    text = str(exc)
    if text.startswith("HTTP ") and ": " in text:
        return text.split(": ", 1)[1]
    return text


# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------


def log_command(
    *,
    command: str,
    user_id: str,
    user_name: str,
    raw: str,
    result: str,
    task_id: str | None = None,
    due: str | None = None,
    note: str | None = None,
) -> None:
    """指令日志：每条指令一行，便于事后排查"用户说他发了 /add 却没建上"。

       指令=add | 用户=我(10001) | 原文=/add 明天下午3点 交实验报告 | 结果=created | 任务id=... | 截止=09-17 15:00
    """
    parts = [
        f"指令={command or '-'}",
        f"用户={user_name}({user_id})",
        f"原文={preview(raw)}",
        f"结果={result}",
    ]
    if task_id:
        parts.append(f"任务id={task_id}")
    if due:
        parts.append(f"截止={due}")
    if note:
        parts.append(f"备注={one_line(note)}")
    logging.getLogger("xcollector.bot.command").info(" | ".join(parts))
