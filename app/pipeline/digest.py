"""每日 digest。

这是任务的**第一出口**：网站需要人主动想起来去看，而 DDL 你没看就没用。
所以 digest 直接推到 QQ 私聊 —— 复用 NapCat 本身，不需要新组件。

尾部固定带一行"盲区"，因为用户必须能区分
"今天确实没通知"和"系统今天瞎了"。

改造点（相对后端里的那一版）：
  - `fetch_all` + `build_views` → `GET /api/notifications`。后端返回的已经是
    **读投影**（人工修正已生效、status 已推导），所以 bot 不再需要自己组装视图；
  - 统计与缺口分别走 `GET /api/stats`、`GET /api/gap-alerts`；
  - **"今天发过没有"问后端的 `digest_log`**（契约第 10 节）。为什么不做成
    bot 内存标记：bot 重启后那个标记就没了，当天的 digest 会被重发一遍 ——
    对收件人是骚扰，比漏发更糟。所以这条状态必须放在后端。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from ..backend_client import BackendClient, BackendError
from ..config import Settings, get_settings
from ..onebot.hub import OneBotNotConnected, interpret_send_result
from ..utils import local_day, now_ms, to_local

logger = logging.getLogger(__name__)

MAX_LEN = 1800


# ---------------------------------------------------------------------------
# 运行上下文：digest 需要"读后端 + 用 OneBot 发私聊"两样东西
# ---------------------------------------------------------------------------


@dataclass
class DigestContext:
    backend: BackendClient
    sender: Any  # OneBotHub（或任何有 send_private_msg 的对象，方便测试替身）


_context: DigestContext | None = None


def configure_digest(backend: BackendClient, sender: Any) -> None:
    """由 main.BotRuntime 在启动时调用一次。"""
    global _context
    _context = DigestContext(backend=backend, sender=sender)


def get_digest_context() -> DigestContext:
    if _context is None:
        raise RuntimeError("digest 上下文还没有配置：请先调用 configure_digest()")
    return _context


def reset_digest_context() -> None:
    global _context
    _context = None


# ---------------------------------------------------------------------------
# 组装文本
# ---------------------------------------------------------------------------


def _fmt_due(view: dict) -> str:
    due_at = view.get("due_at")
    text = view.get("due_text") or ""
    conf = view.get("due_confidence") or 0.0

    if due_at is None:
        return f"时间待确认（原文「{text}」）" if text else "无明确时间"

    dt = to_local(due_at)
    stamp = dt.strftime("%m-%d(%a) %H:%M") if dt else "?"

    if conf < 0.6:
        return f"{stamp} 待确认"
    if conf < 0.9:
        return f"~{stamp}"
    return stamp


async def build_digest(
    backend: BackendClient | None = None,
    settings: Settings | None = None,
    *,
    user_id: str,
) -> str:
    """读后端 → 组装**某个用户**今天的 digest 文本。**不发送。**

    `user_id` 必填：多用户之后"今天新增几条"是**每个人的**事。
    以前这里读的是全站通知，现在读的是这个人的读投影（后端按 user_id 隔离），
    所以 A 的 digest 里绝不会出现 B 订阅的来源。
    """
    settings = settings or get_settings()
    if backend is None:
        backend = get_digest_context().backend

    today = local_day()
    now = now_ms()
    day_start = now - 24 * 3600 * 1000

    # 一次把全部通知拉回来自己分组。MAX_LEN 只约束发出去的文本，
    # 这里多拿一些是为了让"今天新增"和"24 小时内到期"两份清单都准。
    views = await backend.list_notifications(user_id=user_id, status="all", limit=2000)

    fresh = [
        v for v in views
        if (v.get("created_at") or 0) >= day_start and v.get("status") != "archived"
    ]
    fresh.sort(key=lambda v: v.get("due_at") is None)
    due_soon = [
        v for v in views
        if v.get("status") == "active" and v.get("due_at") and now <= v["due_at"] <= now + 24 * 3600 * 1000
    ]
    expired = [
        v for v in views
        if v.get("status") == "expired" and v.get("due_at") and v["due_at"] >= day_start
    ]

    lines: list[str] = [f"【Xcollector 每日通知】{today}", ""]

    lines.append(f"■ 新增 {len(fresh)} 条")
    if not fresh:
        lines.append("  （无）")
    for v in fresh[:12]:
        lines.append(f"· {v.get('title')}")
        lines.append(f"  截止 {_fmt_due(v)}")
        if v.get("location"):
            lines.append(f"  地点 {v['location']}")
        lines.append(f"  来源 {v.get('group_name') or v.get('group_id')} · {v.get('sender_name') or ''}")
        lines.append(f"  「{(v.get('evidence') or '')[:60]}」")
    if len(fresh) > 12:
        lines.append(f"  …另有 {len(fresh) - 12} 条，请到网站查看")

    if due_soon:
        lines += ["", f"■ 24 小时内到期 {len(due_soon)} 条"]
        for v in due_soon[:8]:
            lines.append(f"· {v.get('title')} — {_fmt_due(v)}")

    if expired:
        lines += ["", f"■ 已过期 {len(expired)} 条（确认或归档）"]
        for v in expired[:5]:
            lines.append(f"· {v.get('title')} — {_fmt_due(v)}")

    # ---------------- 盲区 ----------------
    # 盲区也是**按用户**的：只有这个人的源里没解析出来的才算他的盲区。
    # 后端按 user_id 过滤，所以这里拿到的就是"我的源今天出过什么问题"。
    try:
        stat = await backend.get_stats(today, user_id=user_id)
    except BackendError as exc:
        logger.warning("读统计失败 user=%s：%s", user_id, exc)
        stat = {}
    try:
        gaps = await backend.list_gap_alerts(user_id=user_id, limit=5)
    except BackendError as exc:
        logger.warning("读缺口告警失败 user=%s：%s", user_id, exc)
        gaps = []

    unparsed = int(stat.get("unparsed") or 0)
    conflicts = int(stat.get("conflicts") or 0)
    degraded = int(stat.get("degraded") or 0)

    lines += ["", "■ 本系统今日盲区"]
    lines.append(
        f"· 收到 {stat.get('ingested', 0)} 条，抽出 {stat.get('extracted', 0)} 条，"
        f"未能解析 {unparsed} 条"
    )
    if conflicts:
        lines.append(f"· {conflicts} 条两个模型给的 DDL 不一致，已标红待确认")
    if degraded:
        lines.append(f"· {degraded} 条在模型失败时降级为规则处理，可能有误")
    if gaps:
        for g in gaps[:3]:
            hours = round((int(g.get("to_ts") or 0) - int(g.get("from_ts") or 0)) / 3600000, 1)
            lines.append(f"⚠ 群「{g.get('group_name') or g.get('group_id')}」有 {hours} 小时消息缺口，请手工核对")
    else:
        lines.append("· 未检测到消息缺口")
    if not conflicts and not degraded and not unparsed and not gaps:
        lines.append("· 本日无异常")

    lines += ["", f"（服务器时间 {to_local(now).strftime('%Y-%m-%d %H:%M')}）"]

    text = "\n".join(lines)
    if len(text) > MAX_LEN:
        text = text[: MAX_LEN - 20] + "\n…（已截断，详情见网站）"
    return text


# ---------------------------------------------------------------------------
# 发送
# ---------------------------------------------------------------------------


async def _send_private(sender: Any, target: str, text: str) -> tuple[bool, str | None]:
    """用 OneBot hub 发私聊。**不抛异常**，返回 (成功?, 错误)。"""
    try:
        resp = await sender.send_private_msg(target, text)
    except OneBotNotConnected as exc:
        return False, f"OneBot 未连接：{exc}"
    except asyncio.TimeoutError:
        return False, "发送超时：NapCat 没有返回这次调用"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    problem = interpret_send_result(resp)
    if problem:
        return False, f"NapCat 返回失败：{problem}"
    return True, None


async def resolve_recipients(
    backend: BackendClient, settings: Settings | None = None
) -> list[tuple[str, str]]:
    """定出这次 digest 该发给谁，返回 `[(qq, user_id)]`。

    两种模式：

      - **默认（多用户）**：所有 active 的注册用户，各发一份**自己那份**。
        digest 是"你的 DDL"，不是"全站的 DDL"，所以文本也是各人各组装。
      - **`DIGEST_TARGET_QQ` 配了**：只发那一个 QQ。这是给"实际上就一个人用"
        或者迁移期用的，行为和这个字段在单租户时代完全一样。

    收件人必须是**注册用户**：digest 的正文来自那个人的通知，没有账号就没有
    可组装的正文。以前 `DIGEST_TARGET_QQ` 可以是任意 QQ（发的是全站摘要），
    现在不行了 —— 这种情况会记 ERROR 而不是安静地不发。
    """
    settings = settings or get_settings()
    target = (settings.digest_target_qq or "").strip()

    if target:
        user = await backend.get_user_by_qq(target)
        if not user:
            logger.error(
                "DIGEST_TARGET_QQ=%s 不是一个注册用户，digest 发不出去。"
                "多用户之后 digest 是按人组装的，收件人必须先注册（或把这个配置留空）",
                target,
            )
            return []
        return [(target, str(user.get("id") or ""))]

    users = await backend.list_users()
    out: list[tuple[str, str]] = []
    for user in users:
        qq = str(user.get("qq") or "").strip()
        uid = str(user.get("id") or "").strip()
        status = str(user.get("status") or "active")
        if not qq or not uid:
            continue
        if status != "active":
            # 被停用的账号不发：发出去也没有意义，而且会让人以为账号还在用
            continue
        out.append((qq, uid))
    return out


def _empty_result(error: str, *, dry_run: bool) -> dict:
    """一条都没发出去时的返回值。

    **形状必须和成功路径完全一致**（ok/sent/total/dry_run/text/recipients/error）。
    少一个键，调用方（网页上的预览按钮、自动发送循环）就会在"没人可发"这条
    路径上抛 KeyError —— 而那正好是最需要它好好报错的时候。
    """
    return {
        "ok": False,
        "sent": 0,
        "total": 0,
        "dry_run": bool(dry_run),
        "text": "",
        "recipients": [],
        "error": error,
    }


async def send_digest(
    dry_run: bool = True,
    kind: str = "manual",
    *,
    user_id: str | None = None,
    qq: str | None = None,
    backend: BackendClient | None = None,
    sender: Any | None = None,
) -> dict:
    """组装并（可选）发送 digest。返回值即 `POST /api/digest/send` 的响应体。

    不指定收件人时按 `resolve_recipients` 逐个发。指定了（`user_id` + `qq`）
    就只发那一个 —— 单用户部署和排查用得上。

    `dry_run=True` 时只组装不发，但**仍然写一条 preview 记录**：
    "预览过什么" 也是需要能回看的事实。
    """
    settings = get_settings()
    if backend is None or sender is None:
        ctx = get_digest_context()
        backend = backend or ctx.backend
        sender = sender or ctx.sender

    today = local_day()

    if user_id and qq:
        targets: list[tuple[str, str]] = [(qq, user_id)]
    elif user_id:
        # 从用户表把这个人的 QQ 找回来，省得调用方自己拼
        users = await backend.list_users()
        match = [str(u.get("qq") or "") for u in users if str(u.get("id") or "") == user_id]
        if not match or not match[0]:
            error = f"找不到 user_id={user_id} 对应的 QQ"
            logger.warning("digest 未发送：%s", error)
            return _empty_result(error, dry_run=dry_run)
        targets = [(match[0], user_id)]
    else:
        try:
            targets = await resolve_recipients(backend, settings)
        except BackendError as exc:
            error = f"读取收件人失败：{exc}"
            logger.warning(error)
            return _empty_result(error, dry_run=dry_run)

    if not targets:
        error = "没有可发送的收件人（还没有注册用户，或 DIGEST_TARGET_QQ 不是注册用户）"
        logger.warning("digest 未发送：%s", error)
        return _empty_result(error, dry_run=dry_run)

    results: list[dict] = []
    for target_qq, target_uid in targets:
        try:
            text = await build_digest(backend, settings, user_id=target_uid)
        except BackendError as exc:
            # 读不到通知就先不发 —— 发一条"新增 0 条"的空 digest 比不发更误导人
            error = f"读取通知失败：{exc}"
            await _log_digest(backend, target_uid, today, kind, "", sent=False, error=error)
            results.append({"qq": target_qq, "user_id": target_uid, "sent": False, "error": error})
            continue

        if dry_run:
            await _log_digest(backend, target_uid, today, "preview", text, sent=False, error=None)
            results.append({"qq": target_qq, "user_id": target_uid, "sent": False, "text": text, "error": None})
            continue

        sent, error = await _send_private(sender, target_qq, text)
        if not sent and not error:
            error = "发送失败，但没有给出原因"
        await _log_digest(backend, target_uid, today, kind, text, sent=sent, error=error)
        if not sent:
            # 发不出去最常见的原因是"对方不是机器人的好友"：QQ 机器人**不能**
            # 主动给陌生人发消息。这条日志是唯一能让人看出原因的线索。
            logger.warning(
                "digest 发不出去 qq=%s user=%s：%s（QQ 机器人只能给好友或临时会话发消息）",
                target_qq,
                target_uid,
                error,
            )
        results.append({"qq": target_qq, "user_id": target_uid, "sent": sent, "text": text, "error": error})

    sent_count = sum(1 for r in results if r["sent"])
    failed = [r for r in results if not r["sent"]] if not dry_run else []
    return {
        "ok": (sent_count == len(results)) if not dry_run else True,
        "sent": sent_count,
        "total": len(results),
        "dry_run": bool(dry_run),
        # 文本只回第一份：它是"这一批长什么样"的样本，逐份回会在管理页上刷屏。
        # 每份都写进了 digest_log，要核对细节看那里。
        "text": (results[0].get("text") or "") if results else "",
        "recipients": [{k: v for k, v in r.items() if k != "text"} for r in results],
        "error": (failed[0].get("error") if failed else None),
    }


async def _log_digest(
    backend: BackendClient,
    user_id: str,
    day: str,
    kind: str,
    text: str,
    *,
    sent: bool,
    error: str | None,
) -> None:
    """把这次发送记进后端的 digest_log。

    写失败也**不抛**：发送本身已经发生了，日志是附属品。
    """
    try:
        await backend.add_digest_log(
            user_id=user_id, day=day, kind=kind, text=text, sent=sent, error=error
        )
    except BackendError as exc:
        logger.warning("写 digest_log 失败（user=%s day=%s kind=%s）：%s", user_id, day, kind, exc)


# ---------------------------------------------------------------------------
# 调度
# ---------------------------------------------------------------------------


async def sent_today(backend: BackendClient, user_id: str, day: str | None = None) -> bool:
    """**这个用户**今天自动 digest 是否已经发成功过（问后端，不问内存）。

    必须按用户问：A 收到过不代表 B 收到过。混成一个"今天发过了"的标记，
    会让第二个人的那份被静默吞掉 —— 而"没收到"过很久才会被发现。
    """
    try:
        return (
            await backend.count_digest_logs(
                user_id=user_id, day=day or local_day(), kind="auto", sent=True
            )
            > 0
        )
    except BackendError as exc:
        logger.warning("查询 digest 发送记录失败（user=%s），保守判定为未发送：%s", user_id, exc)
        return False


async def auto_attempts_today(backend: BackendClient, user_id: str, day: str | None = None) -> int:
    try:
        return await backend.count_digest_logs(
            user_id=user_id, day=day or local_day(), kind="auto"
        )
    except BackendError as exc:
        logger.warning("查询 digest 尝试次数失败（user=%s）：%s", user_id, exc)
        return 0


async def digest_loop() -> None:
    """每分钟检查一次是否到了发送时间，到点了给**每个还没收到的用户**发。"""
    settings = get_settings()
    if not settings.digest_enabled:
        logger.info("每日 digest 未启用")
        return

    hh, mm = settings.digest_hhmm
    logger.info("每日 digest 已启用：%02d:%02d（%s）", hh, mm, settings.digest_tz)
    backend = get_digest_context().backend
    max_attempts = settings.digest_max_auto_attempts
    warned = False

    while True:
        try:
            now = to_local(now_ms())
            today = local_day()
            if now and (now.hour, now.minute) >= (hh, mm):
                try:
                    targets = await resolve_recipients(backend, settings)
                except BackendError as exc:
                    logger.warning("取 digest 收件人失败，本轮跳过：%s", exc)
                    targets = []

                # 逐个用户判断"这个人今天发过没有"：先发的先成功，
                # 后面失败的下一分钟还会被捡起来，不会因为别人成功而被跳过。
                pending: list[tuple[str, str]] = []
                for target_qq, target_uid in targets:
                    if not await sent_today(backend, target_uid, today):
                        pending.append((target_qq, target_uid))

                if pending:
                    already = await auto_attempts_today(backend, pending[0][1], today)
                    if already >= max_attempts:
                        if not warned:
                            warned = True
                            logger.error(
                                "每日 digest 对部分用户今日已失败 %d 次，不再重试；"
                                "请检查 OneBot 连接、对方是否是机器人好友，以及后端 digest_log",
                                already,
                            )
                    else:
                        for target_qq, target_uid in pending:
                            result = await send_digest(
                                dry_run=False,
                                kind="auto",
                                user_id=target_uid,
                                qq=target_qq,
                            )
                            if result.get("sent"):
                                logger.info("每日 digest 已发送 user=%s", target_uid)
                            else:
                                logger.warning(
                                    "每日 digest 发送失败 user=%s：%s",
                                    target_uid,
                                    result.get("error"),
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("digest 循环异常：%s", exc)
        await asyncio.sleep(60)
