"""私聊指令路由。

设计原则：
  - **准入先于一切**：不在 COMMAND_WHITELIST 里的私聊消息直接忽略（只记 DEBUG）。
    不回复、不报错 —— 回复等于告诉陌生人"这里有个机器人"。
  - **解析是纯函数**：parse_command / is_yes / is_no / 各种 format_* 都不碰网络，
    所以 tests/check_commands.py 能在没有 NapCat、没有 backend 的机器上跑。
  - **状态可丢**：待确认的 /add 和 /list 的编号映射都在内存里，重启即失。
    这是刻意的：bot 不该有数据库，有状态的部分都在 backend。
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
    "/help          显示这条帮助"
)

# 固定用 UTC+8 渲染时间，和 backend 的 DIGEST_TZ 默认值保持一致。
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
    return ParsedCommand(name=head.lower(), arg=arg, raw=raw)


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

    存的是**原始文本**而不是解析结果：确认时要让 backend 用同一段文本
    重新走一遍（带 force_commit），这样才能保证"用户看到的预览"
    和"最终建出来的任务"出自同一次解析。
    """

    text: str
    created_at: float  # time.monotonic()
    preview: dict


def pending_expired(
    pending: PendingAdd, ttl_seconds: float, now: float | None = None
) -> bool:
    """待确认状态是否过期（纯函数，方便离线测试）。

    用 monotonic 而不是墙钟：用户改系统时间不该让确认窗口凭空失效。
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
        self._pending: dict[str, PendingAdd] = {}
        self._listings: dict[str, list[ListedItem]] = {}

    # ---------------- 入口 ----------------

    async def handle_private_event(self, event: dict) -> None:
        """处理一条私聊事件。非私聊直接返回。"""
        if message_kind(event) != "private":
            return

        user_id = str(event.get("user_id") or "")
        if not self.settings.in_command_whitelist(user_id):
            # 只记 DEBUG：这类消息可能来自任意陌生人，INFO 级别会被刷爆，
            # 而且我们也不想在日志里留下"机器人回复过谁"的痕迹
            logger.debug("忽略非白名单私聊 user=%s", user_id)
            return

        user_name = self.settings.command_whitelist_map.get(user_id, user_id)
        parsed = parse_message(event.get("message") or event.get("raw_message") or [])
        text = (parsed.text or "").strip()
        if not text:
            return

        command = parse_command(text, self.settings.command_prefix)
        try:
            if command is None:
                await self._maybe_confirm(user_id, user_name, text)
                return
            await self._dispatch(command, user_id, user_name)
        except BackendRejected as exc:
            # 后端明确拒绝（参数不对 / 对象不存在）：把原因说清楚，别甩"稍后再试"
            await self._reply(user_id, f"⚠️ 操作被后端拒绝：{exc}")
            log_command(
                command=command.name if command else "-",
                user_id=user_id,
                user_name=user_name,
                raw=text,
                result="error",
                note=str(exc),
            )
        except BackendError as exc:
            await self._reply(user_id, BACKEND_DOWN_REPLY)
            log_command(
                command=command.name if command else "-",
                user_id=user_id,
                user_name=user_name,
                raw=text,
                result="error",
                note=str(exc),
            )
        except Exception as exc:  # 兜底：任何意外都要让用户知道"这条没成"
            logger.exception("指令处理失败: %s", exc)
            await self._reply(user_id, "⚠️ 处理这条指令时出错了，请稍后再试。")
            log_command(
                command=command.name if command else "-",
                user_id=user_id,
                user_name=user_name,
                raw=text,
                result="error",
                note=f"{type(exc).__name__}: {exc}",
            )

    async def _dispatch(self, command: ParsedCommand, user_id: str, user_name: str) -> None:
        name = command.name
        if name == "help":
            await self._reply(user_id, HELP_TEXT)
            log_command(
                command=name, user_id=user_id, user_name=user_name,
                raw=command.raw, result="help",
            )
        elif name == "add":
            await self._handle_add(command, user_id, user_name)
        elif name == "list":
            await self._handle_list(command, user_id, user_name)
        elif name in ("done", "del"):
            await self._handle_mark(command, user_id, user_name, name)
        elif name == "cancel":
            self._pending.pop(user_id, None)
            await self._reply(user_id, "已取消。")
            log_command(
                command=name, user_id=user_id, user_name=user_name,
                raw=command.raw, result="cancelled",
            )
        else:
            await self._reply(user_id, UNKNOWN_COMMAND_REPLY)
            log_command(
                command=name, user_id=user_id, user_name=user_name,
                raw=command.raw, result="unknown",
            )

    # ---------------- /add ----------------

    async def _handle_add(
        self, command: ParsedCommand, user_id: str, user_name: str
    ) -> None:
        text = command.arg.strip()
        if not text:
            await self._reply(user_id, NO_ARG_ADD_REPLY)
            log_command(
                command="add", user_id=user_id, user_name=user_name,
                raw=command.raw, result="error", note="缺少内容",
            )
            return

        body = await self.backend.create_manual_task(
            text=text,
            sender_id=user_id,
            sender_name=user_name,
            auto_commit=True,
        )

        if not body.get("ok"):
            await self._reply(
                user_id,
                f"⚠️ 没能添加：{body.get('error') or '后端没有返回原因'}",
            )
            log_command(
                command="add", user_id=user_id, user_name=user_name,
                raw=command.raw, result="error", note=str(body.get("error")),
            )
            return

        task = body.get("task") or {}
        preview_data = body.get("preview") or {}

        if body.get("needs_confirm") and not task:
            # 挂起：把原文存起来，用户回 y 时用同一段原文重放
            self._pending[user_id] = PendingAdd(
                text=text, created_at=time.monotonic(), preview=preview_data
            )
            await self._reply(user_id, format_confirm_prompt(preview_data))
            log_command(
                command="add", user_id=user_id, user_name=user_name,
                raw=command.raw, result="needs_confirm",
                due=format_due(
                    preview_data.get("due_at"),
                    preview_data.get("due_text"),
                    preview_data.get("due_confidence") or 0.0,
                ),
            )
            return

        self._pending.pop(user_id, None)
        await self._reply(user_id, format_add_receipt(task or preview_data))
        log_command(
            command="add", user_id=user_id, user_name=user_name,
            raw=command.raw, result="created",
            task_id=task.get("id"),
            due=format_due(
                (task or preview_data).get("due_at"),
                (task or preview_data).get("due_text"),
                (task or preview_data).get("due_confidence") or 0.0,
            ),
        )

    async def _maybe_confirm(self, user_id: str, user_name: str, text: str) -> None:
        """处理"不是指令"的私聊：可能是对 /add 的 y/n 回答，也可能什么都不是。

        为什么 y/n 要走这条分支而不是做成指令：用户被问了"仍然添加吗"之后，
        打的就是一个 y，让他再打 "/y" 是反人性的。
        """
        pending = self._pending.get(user_id)
        if pending is not None and (is_yes(text) or is_no(text)):
            if pending_expired(pending, self.settings.pending_ttl_seconds):
                self._pending.pop(user_id, None)
                await self._reply(
                    user_id,
                    f"⏳ 上一条 /add 已经超过 {self.settings.pending_ttl_seconds // 60} 分钟，已作废，请重新发送。",
                )
                log_command(
                    command="add", user_id=user_id, user_name=user_name,
                    raw=text, result="cancelled", note="待确认超时",
                )
                return
            await self._confirm(user_id, user_name, text, yes=is_yes(text))
            return

        # 既不是指令也不是 y/n —— 用户大概率只是打错字了，不回话
        logger.debug("忽略无法识别的私聊 user=%s text=%s", user_id, preview(text))

    async def _confirm(self, user_id: str, user_name: str, text: str, yes: bool) -> None:
        pending = self._pending.pop(user_id, None)
        if pending is None:
            return
        if not yes:
            await self._reply(user_id, "已取消。")
            log_command(
                command="add", user_id=user_id, user_name=user_name,
                raw=text, result="cancelled", note="用户放弃待确认的添加",
            )
            return

        body = await self.backend.create_manual_task(
            text=pending.text,
            sender_id=user_id,
            sender_name=user_name,
            auto_commit=False,
            force_commit=True,
        )
        task = body.get("task") or {}
        if not body.get("ok") or not task:
            await self._reply(
                user_id, f"⚠️ 没能添加：{body.get('error') or '后端没有返回任务'}"
            )
            log_command(
                command="add", user_id=user_id, user_name=user_name,
                raw=text, result="error", note=str(body.get("error")),
            )
            return

        await self._reply(user_id, format_add_receipt(task))
        log_command(
            command="add", user_id=user_id, user_name=user_name,
            raw=text, result="confirmed", task_id=task.get("id"),
            due=format_due(
                task.get("due_at"), task.get("due_text"), task.get("due_confidence") or 0.0
            ),
        )

    # ---------------- /list ----------------

    async def _handle_list(
        self, command: ParsedCommand, user_id: str, user_name: str
    ) -> None:
        n = parse_list_n(command.arg)
        items = await self.backend.list_notifications(status="active", limit=LIST_FETCH_LIMIT)
        total = len(items)
        selected = items[:n]
        # 编号→通知 id 的映射**按用户存**：同一个群里两个人都发 /list，
        # 各自的"3 号"必须是各自看到的那条
        self._listings[user_id] = [
            ListedItem(notif_id=str(it.get("id")), title=one_line(it.get("title") or ""))
            for it in selected
        ]
        await self._reply(
            user_id,
            format_list_reply(selected, total=total, shown=len(selected), prefix=self.settings.command_prefix),
        )
        log_command(
            command="list", user_id=user_id, user_name=user_name,
            raw=command.raw, result="listed", note=f"共 {total} 条，显示 {len(selected)}",
        )

    # ---------------- /done /del ----------------

    async def _handle_mark(
        self, command: ParsedCommand, user_id: str, user_name: str, name: str
    ) -> None:
        label = "done" if name == "done" else "archived"
        listing = self._listings.get(user_id) or []
        if not command.arg:
            await self._reply(
                user_id, NO_ARG_DONE_REPLY if name == "done" else NO_ARG_DEL_REPLY
            )
            log_command(
                command=name, user_id=user_id, user_name=user_name,
                raw=command.raw, result="error", note="缺少编号",
            )
            return

        index = parse_index(command.arg)
        if index is None or index > len(listing):
            hint = "先发 /list 看看有哪些待办。" if not listing else f"编号要在 1~{len(listing)} 之间。"
            await self._reply(user_id, f"⚠️ 编号无效。{hint}")
            log_command(
                command=name, user_id=user_id, user_name=user_name,
                raw=command.raw, result="error", note="编号无效",
            )
            return

        item = listing[index - 1]
        await self.backend.correct_notification(
            item.notif_id,
            field="status",
            value=label,
            user_id=f"qq:{user_id}",
        )
        if name == "done":
            await self._reply(user_id, f"✅ 已完成：{item.title or item.notif_id[-6:]}")
        else:
            await self._reply(user_id, f"🗑 已移除：{item.title or item.notif_id[-6:]}")
        log_command(
            command=name, user_id=user_id, user_name=user_name,
            raw=command.raw, result=label, task_id=item.notif_id,
        )

    # ---------------- 回复 ----------------

    async def _reply(self, user_id: str, text: str) -> None:
        payload = truncate(text, MAX_REPLY_CHARS, TRUNCATE_SUFFIX)
        try:
            await self.sender.send_private_msg(user_id, payload)
        except Exception as exc:
            # 发不出去只说明 NapCat 掉了；指令本身的结果已经记在日志里了
            logger.warning("回复私聊失败 user=%s: %s", user_id, exc)


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

       指令=add | 用户=我(242684313) | 原文=/add 明天下午3点 交实验报告 | 结果=created | 任务id=... | 截止=09-17 15:00
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


def log_forward(
    *,
    ok: bool,
    group_id: str,
    group_name: str | None,
    sender_name: str,
    message_id: str,
    text: str,
    note: str | None = None,
) -> None:
    """群消息转发日志：每条一行。

       转发=ok | 群=NOVA官方通知群(673504310) | 发送者=李老师 | msg_id=12345 | 原文=大家下周三前…
    """
    parts = [
        "转发=" + ("ok" if ok else "fail"),
        f"群={group_name}({group_id})" if group_name else f"群={group_id}",
        f"发送者={sender_name}",
        f"msg_id={message_id}",
        f"原文={preview(text)}",
    ]
    if note:
        parts.append(f"备注={one_line(note)}")
    logging.getLogger("xcollector.bot.forward").info(" | ".join(parts))
