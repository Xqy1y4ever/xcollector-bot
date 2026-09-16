"""缺口 / 静默看门狗。

NapCat 靠实时事件推送，历史消息拉取能力有限且不稳定。
这意味着 **bot 挂掉的那几个小时里的官方通知会永久消失**，而重启后一切看起来正常。

既然"官方通知恰好就是不能漏的那些"，系统必须主动承认自己的盲区，
而不是安静地继续工作。

改造点（相对后端里的那一版）：
  - 数据源从"本地 group_state 表"换成 `GET /api/groups`；
  - 去重从"查本地 gap_alert 表"换成 `GET /api/gap-alerts?acknowledged=false`；
  - 落库从 `add_gap_alert(...)` 换成 `POST /api/gap-alerts`。

和"两条消息间隔过久"那一种（runner 里做的）分工不同：
  - runner：**群还在说话**，只是中间空了一大段 → 说明 bot 那段没在听；
  - watchdog：**群已经不说话了**，安静到让人觉得不对劲。
"""

from __future__ import annotations

import asyncio
import logging

from ..backend_client import BackendClient, BackendError
from ..config import Settings, get_settings
from ..utils import now_ms

logger = logging.getLogger(__name__)

DEDUPE_HOURS = 6
CHECK_INTERVAL = 15 * 60
STARTUP_DELAY = 30


async def _recent_alert_exists(backend: BackendClient, group_id: str) -> bool:
    """近 DEDUPE_HOURS 小时内是否已经为这个群告警过（避免每 15 分钟刷一条）。"""
    try:
        alerts = await backend.list_gap_alerts(acknowledged=False, limit=50)
    except BackendError as exc:
        # 查不到就当作"没有旧告警"。宁可偶尔重复告警，也不要因为读失败而漏报缺口。
        logger.warning("读取缺口告警失败：%s", exc)
        return False
    cutoff = now_ms() - DEDUPE_HOURS * 3600 * 1000
    for alert in alerts:
        if str(alert.get("group_id")) != str(group_id):
            continue
        created = alert.get("created_at") or 0
        try:
            if int(created) > cutoff:
                return True
        except (TypeError, ValueError):
            continue
    return False


async def check_silence(
    backend: BackendClient,
    reason: str = "silence",
    settings: Settings | None = None,
) -> int:
    """检查白名单群是否长时间没有消息。返回新增告警数。"""
    settings = settings or get_settings()
    threshold_ms = settings.gap_alert_hours * 3600 * 1000
    now = now_ms()
    created = 0

    try:
        groups = await backend.list_groups()
    except BackendError as exc:
        logger.warning("静默检查：读群状态失败，跳过本轮：%s", exc)
        return 0

    states = {str(g.get("group_id")): g for g in groups}

    # 白名单里有、但从未收到过消息的群**不做静默判断**：没有 last_msg_ts
    # 就分不清"这个群一直很安静"和"bot 从没连上过"，硬报会有大量误报。
    # 真正的"从没收到过消息"由 /api/status 的 groups 列表暴露出来。
    for gid, name in settings.group_whitelist_map.items():
        state = states.get(gid)
        if state is None:
            continue
        last_ts = state.get("last_msg_ts")
        if not last_ts:
            continue
        try:
            silent_ms = now - int(last_ts)
        except (TypeError, ValueError):
            continue
        if silent_ms <= threshold_ms:
            continue
        if await _recent_alert_exists(backend, gid):
            continue
        try:
            await backend.create_gap_alert(
                group_id=gid,
                group_name=state.get("group_name") or name,
                from_ts=int(last_ts),
                to_ts=now,
                reason=(
                    f"[{reason}] 已 {round(silent_ms / 3600000, 1)} 小时未收到该群任何消息，"
                    "可能是连接中断或 NapCat 掉线，此期间的通知可能已永久丢失"
                ),
            )
        except BackendError as exc:
            logger.warning("写缺口告警失败 group=%s: %s", gid, exc)
            continue
        created += 1
        logger.warning("群 %s 静默 %.1f 小时，已生成缺口告警", gid, silent_ms / 3600000)

    return created


async def startup_gap_check(backend: BackendClient, settings: Settings | None = None) -> None:
    n = await check_silence(backend, reason="startup", settings=settings)
    if n:
        logger.warning("启动检查发现 %d 个群存在消息缺口", n)
    else:
        logger.info("启动检查：未发现消息缺口")


async def silence_loop(backend: BackendClient, settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    await asyncio.sleep(STARTUP_DELAY)
    while True:
        try:
            await check_silence(backend, reason="periodic", settings=settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("静默检查异常：%s", exc)
        await asyncio.sleep(CHECK_INTERVAL)
