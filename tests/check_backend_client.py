"""bot 的 BackendClient 对着**真后端**跑一遍：这是两个仓库之间唯一的接口。

需要一个**正在运行的后端**：

    $env:API_TOKEN='service-token'; $env:SIGNUP_MODE='invite'; $env:SERVER_PORT='8005'
    .\\xcollector-backend\\.venv\\Scripts\\python.exe -m app.main

    $env:CLIENT_BASE='http://127.0.0.1:8005'; $env:CLIENT_SERVICE_TOKEN='service-token'
    .\\.venv\\Scripts\\python.exe -m tests.check_backend_client

## 为什么单独测这一层

`tests/check_pipeline_e2e.py` 跑的是 `app.tools.fake_backend` —— 一个**仿制品**。
仿制品和真后端一旦漂移，e2e 全绿而线上全挂，而且没人会发现。

多用户改造正好改的就是这一层的形状：按用户的接口全部多了一个 `user_id` query
参数，`correction.user_id` 还改了含义（现在是租户，操作者叫 `actor`）。
少传、传错、把 actor 当 user_id 传 —— 这几种错在 bot 侧看起来都"请求成功了"。

所以这个文件**只跟真后端说话**，把 bot 侧每一个按用户的调用都过一遍，
并且验证两件事：数据确实按用户分开了、跨用户读确实读不到。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx

from app.backend_client import BackendClient, BackendError, BackendRejected
from app.config import Settings

BASE = os.environ.get("CLIENT_BASE", "http://127.0.0.1:8005").rstrip("/")
API = BASE + "/api"
SERVICE = os.environ.get("CLIENT_SERVICE_TOKEN", "service-token")

RUN = os.environ.get("CLIENT_RUN") or str(int(time.time() * 1000))
QQ_A = str(600000000 + int(RUN[-7:]) % 30000000)
QQ_B = str(int(QQ_A) + 1)
# 每轮都换，否则上一轮留下的订阅会混进投递名单（测试跑第二遍就红）
_SUF = RUN[-7:]
GROUP = "61" + _SUF
SENDER = "62" + _SUF
OTHER_SENDER = "63" + _SUF

H = {"Authorization": f"Bearer {SERVICE}"}
fails: list[str] = []
total = 0


def check(name: str, got, want) -> None:
    global total
    total += 1
    if got == want:
        print(f"ok    {name}")
    else:
        fails.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f"  {detail}" if detail else ""), bool(cond), True)


def register(qq: str) -> tuple[str, str]:
    """直接用 httpx 注册（注册流程本身由后端的测试守，这里只是在造测试数据）。"""
    c = httpx.Client(timeout=20, trust_env=False)   # 本机假后端不走系统代理
    try:
        code = c.post(f"{API}/verify/request", json={"qq": qq}, headers=H).json()["code"]
        invite = c.post(
            f"{API}/invites", json={"note": f"client-{RUN}", "max_uses": 1}, headers=H
        ).json()["code"]
        r = c.post(f"{API}/register", json={"qq": qq, "code": code, "invite_code": invite})
        assert r.status_code == 200, r.text
        body = r.json()
        return str(body["user"]["id"]), str(body["token"])
    finally:
        c.close()


def notification_payload(raw_id: str, *, title: str) -> dict:
    """`POST /api/notifications` 的请求体（和 runner.build_notification_payload 同形）。"""
    stamp = int(time.time() * 1000)
    return {
        "raw_message_id": raw_id,
        "group_id": GROUP,
        "group_name": "客户端测试群",
        "sender_id": SENDER,
        "sender_name": "客户端测试老师",
        "source_ts": stamp,
        "title": title,
        "summary": None,
        "location": None,
        "due_at": None,
        "due_text": None,
        "due_confidence": 0.0,
        "evidence": f"{title} 的原文证据",
        "conflict": False,
        "candidates": [],
        "extractor": "test",
        "model": None,
        "prompt_ver": "test",
    }


async def main() -> int:
    settings = Settings(
        backend_base_url=BASE,
        api_token=SERVICE,
        backend_timeout=20.0,
    )
    backend = BackendClient(settings)
    print(f"→ {BASE}  RUN={RUN}\n")

    try:
        uid_a, _tok_a = register(QQ_A)
        uid_b, _tok_b = register(QQ_B)
        print(f"    A={uid_a}  B={uid_b}\n")

        # ------------------------------------------------------------------
        print("--- 1. 归属参数不能漏 ---")
        try:
            backend._owner("")
            check_true("空 user_id 就地报错（不浪费一次网络往返）", False, "没有抛异常")
        except ValueError as exc:
            check_true("空 user_id 就地报错", "user_id" in str(exc), str(exc))

        # 服务令牌不带 user_id 时后端必须拒绝 —— 这是"静默串数据"的第一道闸
        try:
            await backend.list_notifications(user_id="", status="all")
            check_true("list_notifications 空 user_id → 报错", False, "居然成功了")
        except ValueError:
            check_true("list_notifications 空 user_id → 报错", True)

        # ------------------------------------------------------------------
        print("\n--- 2. QQ → 用户（bot 的身份解析入口）---")
        user_a = await backend.get_user_by_qq(QQ_A)
        check("查得到 A", (user_a or {}).get("id"), uid_a)
        miss = await backend.get_user_by_qq("19999999999")
        check("查不到的 QQ 返回 None（不是抛异常，也不是空字典）", miss, None)

        # ------------------------------------------------------------------
        print("\n--- 3. 订阅与路由 ---")
        check("还没人订阅时投递名单是空的", await backend.find_subscribers(GROUP, SENDER), [])

        bad = None
        try:
            await backend.add_subscription(uid_a, group_id=GROUP, sender_id="*")
        except BackendRejected as exc:
            bad = str(exc)
        check_true("订整个群被后端拒绝", bad is not None, str(bad))
        check_true(
            "拒绝理由说清了是「整个群」",
            bad is not None and "整个群" in bad,
            str(bad),
        )

        body = await backend.add_subscription(
            uid_a, group_id=GROUP, sender_id=SENDER, group_name="客户端测试群", note="A 的备注"
        )
        check("A 订阅成功", bool(body.get("created")), True)
        sub_a = str((body.get("subscription") or {}).get("id") or "")
        check_true("订阅读到了群名", (body.get("subscription") or {}).get("group_name") == "客户端测试群", str(body))

        again = await backend.add_subscription(uid_a, group_id=GROUP, sender_id=SENDER)
        check("重复订阅是幂等的（created=false，不是报错）", again.get("created"), False)

        check("A 一个人订 → 名单只有 A", await backend.find_subscribers(GROUP, SENDER), [uid_a])
        check("另一个发送者不在名单里", await backend.find_subscribers(GROUP, OTHER_SENDER), [])

        await backend.add_subscription(uid_b, group_id=GROUP, sender_id=SENDER)
        check(
            "B 也订之后名单是并集（**这就是扇出的依据**）",
            sorted(await backend.find_subscribers(GROUP, SENDER)),
            sorted([uid_a, uid_b]),
        )
        # 不给 sender_id = "这个群里任何发送者"，缺口告警用
        check(
            "按群查名单（缺口告警用）也拿到两个人",
            sorted(await backend.find_subscribers(GROUP)),
            sorted([uid_a, uid_b]),
        )

        subs_a = await backend.list_subscriptions(uid_a)
        check("A 只看到自己那 1 条", len(subs_a), 1)
        subs_b = await backend.list_subscriptions(uid_b)
        check("B 也只看到自己那 1 条", len(subs_b), 1)
        check_true("A 的订阅 id 和 B 的不同", sub_a != str(subs_b[0].get("id")), f"{sub_a}")

        # ------------------------------------------------------------------
        print("\n--- 4. 扇出：一条原文，两条各自的通知 ---")
        stamp = int(time.time() * 1000)
        raw = await backend.create_message(
            {
                "message_id": f"client-{RUN}",
                "group_id": GROUP,
                "group_name": "客户端测试群",
                "sender_id": SENDER,
                "sender_name": "客户端测试老师",
                "ts": stamp,
                "content": "客户端测试原文",
                "attachments": [],
                "raw": {"test": True},
            }
        )
        raw_id = str(raw.get("id") or "")
        check_true("原文入库（共享层）", bool(raw_id), str(raw))

        payload = notification_payload(raw_id, title=f"客户端测试-{RUN}")
        n_a = await backend.create_notification(payload, user_id=uid_a)
        n_b = await backend.create_notification(payload, user_id=uid_b)
        check("A 的通知建成功", bool(n_a.get("id")), True)
        check("B 的通知建成功", bool(n_b.get("id")), True)
        check_true("两条通知的 id 不同（各自一行）", n_a.get("id") != n_b.get("id"), f"{n_a.get('id')}")

        same_again = await backend.create_notification(payload, user_id=uid_a)
        check("同一个 (user, raw) 再建一次 → 幂等，不新建", same_again.get("created"), False)
        check("幂等返回的还是原来那条", same_again.get("id"), n_a.get("id"))

        rows_a = await backend.list_notifications(user_id=uid_a, status="all", limit=50)
        rows_b = await backend.list_notifications(user_id=uid_b, status="all", limit=50)
        ids_a = {str(r.get("id")) for r in rows_a}
        ids_b = {str(r.get("id")) for r in rows_b}
        check_true("A 的列表里有 A 那条", str(n_a.get("id")) in ids_a, str(sorted(ids_a)))
        check_true("A 的列表里**没有** B 那条", str(n_b.get("id")) not in ids_a, str(sorted(ids_a)))
        check_true("B 的列表里没有 A 那条", str(n_a.get("id")) not in ids_b, str(sorted(ids_b)))

        detail = await backend.get_notification(str(n_a.get("id")), user_id=uid_a)
        check("A 读得到自己的详情", str((detail.get("notification") or {}).get("id")), str(n_a.get("id")))
        # 详情接口对别人的条目返回 404（id 探测不出来）。
        # 注意 404 在 BackendClient 里属于**可重试**状态（后端刚重启时路由可能
        # 还没挂上），所以它最后是以 BackendUnavailable 抛出来的、状态码 404，
        # 而不是 BackendRejected —— 测试要按这个真实行为断言，不能凭"应该抛
        # BackendRejected"的直觉写。
        leaked = None
        try:
            await backend.get_notification(str(n_a.get("id")), user_id=uid_b)
            leaked = "居然读到了"
        except BackendError as exc:
            leaked = f"{type(exc).__name__}: {exc}"
        check_true("B 读 A 的详情被拒（404）", leaked is not None and "404" in leaked, str(leaked))
        check_true(
            "而且被识别成「对象不存在」而不是「后端挂了」",
            "BackendUnavailable" in str(leaked),
            str(leaked),
        )

        check("A 的计数是 1", await backend.count_notifications(user_id=uid_a, status="all"), 1)

        # ------------------------------------------------------------------
        print("\n--- 5. 人工修正：user_id 是租户，actor 是谁改的 ---")
        corr = await backend.correct_notification(
            str(n_a.get("id")), field="status", value="done", actor=f"qq:{QQ_A}", user_id=uid_a
        )
        check_true("修正写成功", bool(corr), str(corr))
        after = await backend.get_notification(str(n_a.get("id")), user_id=uid_a)
        view = after.get("notification") or {}
        check("A 那条的状态被修正成 done", view.get("status"), "done")

        # actor 不在读投影里，要去修正历史里看。这里直接用 httpx 读回来，
        # 而不是给 BackendClient 加一个 bot 根本用不到的方法。
        # 这一步守的是最危险的一处改名：correction.user_id 原来是"谁改的"，
        # 现在必须是**租户**，操作者叫 actor。写错了整条链路看起来都正常。
        history = httpx.get(
            f"{API}/notifications/{n_a.get('id')}/corrections",
            params={"user_id": uid_a},
            headers=H,
            timeout=20,
            trust_env=False,   # 同上：本机后端不走系统代理
        )
        check("读修正历史 → 200", history.status_code, 200)
        rows = history.json().get("corrections") or history.json().get("items") or []
        actors = [str(r.get("actor") or "") for r in rows]
        check_true(
            f"修正历史里 actor=qq:{QQ_A}（不是被塞进了 user_id）",
            f"qq:{QQ_A}" in actors,
            str(rows)[:220],
        )

        # B 的那条**不受影响** —— 这正是"人工修正不串用户"的核心
        after_b = await backend.get_notification(str(n_b.get("id")), user_id=uid_b)
        check_true(
            "B 那条没被 A 的修正动过",
            (after_b.get("notification") or {}).get("status") != "done",
            str((after_b.get("notification") or {}).get("status")),
        )

        # ------------------------------------------------------------------
        print("\n--- 6. bot 的键值暂存按用户分 ---")
        await backend.put_state("command_pending", QQ_A, {"text": "A 的草稿"}, user_id=uid_a)
        await backend.put_state("command_pending", QQ_A, {"text": "B 的草稿"}, user_id=uid_b)
        got_a = await backend.get_state("command_pending", QQ_A, user_id=uid_a)
        got_b = await backend.get_state("command_pending", QQ_A, user_id=uid_b)
        check("A 读到自己的草稿", (got_a or {}).get("text"), "A 的草稿")
        check("同一个 key 在 B 名下是另一份（没有互相覆盖）", (got_b or {}).get("text"), "B 的草稿")
        items_a = await backend.list_state("command_pending", user_id=uid_a)
        check("A 的 namespace 里只有 1 个键", len(items_a), 1)
        check("删掉 A 的", await backend.delete_state("command_pending", QQ_A, user_id=uid_a), True)
        check("删掉 A 的不影响 B", (await backend.get_state("command_pending", QQ_A, user_id=uid_b) or {}).get("text"), "B 的草稿")

        # ------------------------------------------------------------------
        print("\n--- 7. 统计 / digest_log 按用户分 ---")
        await backend.add_stats(None, {"extracted": 2}, user_id=uid_a)
        await backend.add_stats(None, {"extracted": 5}, user_id=uid_b)
        stats_a = await backend.get_stats(user_id=uid_a)
        stats_b = await backend.get_stats(user_id=uid_b)
        check("A 的统计是 2", int(stats_a.get("extracted") or 0), 2)
        check("B 的统计是 5（没被 A 的盖掉）", int(stats_b.get("extracted") or 0), 5)

        await backend.add_digest_log(
            user_id=uid_a, day="2026-01-01", kind="auto", text="A 的 digest", sent=True
        )
        count_a = await backend.count_digest_logs(user_id=uid_a, day="2026-01-01", kind="auto", sent=True)
        count_b = await backend.count_digest_logs(user_id=uid_b, day="2026-01-01", kind="auto", sent=True)
        check("A 今天发过 1 次", count_a, 1)
        check("B 没发过（**按用户问才不会把别人的算进来**）", count_b, 0)

        # ------------------------------------------------------------------
        print("\n--- 8. 信息源目录 ---")
        sources = await backend.list_sources(keyword="客户端测试群")
        mine = [s for s in sources if s.get("group_id") == GROUP]
        check("目录里有刚见过的来源", len(mine), 1)
        check_true("目录项不含 user_id", mine and "user_id" not in mine[0], str(mine[:1]))

        # ------------------------------------------------------------------
        print("\n--- 9. 退订之后不再投递 ---")
        check("B 退订成功", await backend.delete_subscription(uid_b, str(subs_b[0].get("id"))), True)
        check("名单里只剩 A", await backend.find_subscribers(GROUP, SENDER), [uid_a])
        repeated = None
        try:
            await backend.delete_subscription(uid_b, str(subs_b[0].get("id")))
            repeated = "居然成功了"
        except BackendError as exc:
            repeated = f"{type(exc).__name__}: {exc}"
        check_true("重复删 → 404", repeated is not None and "404" in repeated, str(repeated))

        # ------------------------------------------------------------------
        print("\n--- 10. 健康探针（Docker HEALTHCHECK 走的就是它，不能要 user_id）---")
        health = await backend.health()
        check("health 可达", health.get("reachable"), True)
        check_true("没传 user_id 也不报错（否则容器永远不健康）", health.get("error") is None, str(health.get("error")))
    finally:
        await backend.close()

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
