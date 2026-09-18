"""OneBot 断线重连：退避策略与断开提示。

    python -m tests.check_onebot_reconnect

**为什么要专门测这个**：原来的重连退避在"对端正常关闭"时也会一路涨到 60 秒。
原因是 websockets 在干净关闭时是**抛** `ConnectionClosedOK`，而不是让 `recv()`
返回 `None` —— 所以代码里那句"正常断开就重置退避"永远走不到（异常类型上，
"对端重启"和"连上就被踢"长得一模一样）。

表现出来的现象很误导人：NapCat 重启之后 bot 要等半分钟才重连，而日志上写的是
"正常关闭"。所以要按**这次连接活了多久**来判断，而不是按异常类型。

这里断言三件事：
  1. 退避序列：连上就被踢 → 1/2/4/…/60 递增；稳定运行过 → 回到 1 秒
  2. 关闭码能取出来（不同 websockets 版本放的位置不一样）
  3. 断开提示要指向"下一步查什么"，而不是只丢一个 `1005` 出来
  4. 真的能重连（起一个本地 WS 服务，连上就关）
"""

from __future__ import annotations

import asyncio
import sys
import time

from app.onebot import hub as hub_mod
from app.onebot.hub import (
    STABLE_SECONDS,
    OneBotHub,
    _close_code,
    _explain_disconnect,
    _retry_delay,
)

try:
    from websockets.asyncio.server import serve as _ws_serve
except Exception:  # pragma: no cover - 兼容 websockets 12
    from websockets.server import serve as _ws_serve  # type: ignore

failures: list[str] = []
total = 0


def check(name: str, got, want) -> None:
    global total
    total += 1
    if got == want:
        print(f"ok    {name}")
    else:
        failures.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f"  {detail}" if detail else ""), bool(cond), True)


# ---------------------------------------------------------------------------
print("=== 1. 退避序列 ===")

# 0 = 上一次连接是稳定的（或还没失败过）→ 立刻重连
check("没失败过 → 1s", _retry_delay(0), 1.0)
check("第 1 次连上就被踢 → 1s", _retry_delay(1), 1.0)
check("第 2 次 → 2s", _retry_delay(2), 2.0)
check("第 3 次 → 4s", _retry_delay(3), 4.0)
check("第 4 次 → 8s", _retry_delay(4), 8.0)
check("第 5 次 → 16s", _retry_delay(5), 16.0)
check("第 6 次 → 32s", _retry_delay(6), 32.0)
check("第 7 次 → 60s（封顶）", _retry_delay(7), 60.0)
check("第 20 次仍然是 60s（封顶）", _retry_delay(20), 60.0)
check("负数也当没失败过", _retry_delay(-3), 1.0)

# 关键回归：稳定运行过一段时间之后断开，必须回到 1 秒，
# 而不是把上次连不上时累积的 60 秒带过来。
check_true(
    "稳定连接断开后立刻重连（不是 60s）",
    _retry_delay(0) == 1.0,
    f"STABLE_SECONDS={STABLE_SECONDS}",
)

# ---------------------------------------------------------------------------
print("\n=== 2. 关闭码解析 ===")


class _Frame:
    def __init__(self, code):
        self.code = code


class _Closed(Exception):
    def __init__(self, code=None, *, on_rcvd=True, on_attr=False):
        super().__init__("closed")
        if code is not None:
            if on_rcvd:
                self.rcvd = _Frame(code)
            if on_attr:
                self.code = code


check("从 .rcvd.code 取", _close_code(_Closed(1005)), 1005)
check("从 .code 取（老版本）", _close_code(_Closed(1008, on_rcvd=False, on_attr=True)), 1008)
check("取不到 → None", _close_code(_Closed(None)), None)
check("完全没有这些属性 → None", _close_code(RuntimeError("boom")), None)
check("rcvd 不是数字 → None", _close_code(type("E", (Exception,), {"rcvd": _Frame("x")})()), None)

# ---------------------------------------------------------------------------
print("\n=== 3. 断开提示要能指路 ===")

stable = _explain_disconnect(_Closed(1005), STABLE_SECONDS + 10)
check_true("稳定断开：说明是对端重启/网络抖动", "稳定运行" in stable, stable)
check_true("稳定断开：不吓唬人提 Token", "Token" not in stable, stable)

immediate = _explain_disconnect(_Closed(1005), 0.2)
check_true("刚连上就被关：提到 0.2s", "0.2s" in immediate, immediate)
check_true("刚连上就被关：提示 Token 可能不一致", "ONEBOT_ACCESS_TOKEN" in immediate, immediate)
check_true("刚连上就被关：给出 URL 带 token 的备选写法", "access_token=" in immediate, immediate)
check_true("刚连上就被关：让用户去看 NapCat 日志", "NapCat 自己的日志" in immediate, immediate)
check_true("关闭码 1005 会显示出来", "1005" in immediate, immediate)

no_code = _explain_disconnect(RuntimeError("boom"), 0.1)
check_true("取不到关闭码时写「未给出」而不是崩", "未给出" in no_code, no_code)

attr_code = _explain_disconnect(_Closed(1008, on_rcvd=False, on_attr=True), 0.1)
check_true("1008 也会显示出来", "1008" in attr_code, attr_code)

# ---------------------------------------------------------------------------
print("\n=== 4. 真的会重连（本地 WS 服务，连上就关）===")


async def reconnect_case(hold_seconds: float, run_seconds: float) -> tuple[list[float], OneBotHub]:
    """起一个本地 WS 服务，每次连接后 hold_seconds 秒再关。

    返回每次连接的时间戳与 hub，用来断言重连节奏。
    """
    attempts: list[float] = []

    async def handler(ws, *args):  # 老版本会多传一个 path
        attempts.append(time.monotonic())
        if hold_seconds > 0:
            await asyncio.sleep(hold_seconds)
        await ws.close()

    server = await _ws_serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        settings = hub_mod.get_settings().model_copy(
            update={"onebot_mode": "client", "onebot_ws_url": f"ws://127.0.0.1:{port}"}
        )
        hub = OneBotHub(settings)
        await hub.start()
        await asyncio.sleep(run_seconds)
        await hub.stop()
        return attempts, hub
    finally:
        server.close()
        await server.wait_closed()


async def main_async() -> int:
    # --- 连上就被踢：应当重连，而且退避在增长 ---
    attempts, hub = await reconnect_case(hold_seconds=0, run_seconds=4.0)
    check_true("连上就被踢：发生了多次重连", len(attempts) >= 3, f"{len(attempts)} 次")
    check_true("连上就被踢：reconnect_count 跟着涨", hub.reconnect_count >= 3, str(hub.reconnect_count))
    check_true("连上就被踢：last_error 记了原因", bool(hub.last_error), repr(hub.last_error))
    check_true(
        "连上就被踢：last_error 是 ConnectionClosed 系列",
        "ConnectionClosed" in (hub.last_error or ""),
        repr(hub.last_error),
    )
    check("连上就被踢：没有假装自己是连着的", hub.connected, False)

    # --- 稳定运行后才断开：必须**立刻**重连，不能带着退避 ---
    # 把"稳定"的门槛压到 0.3s，让测试跑得快；服务端保持 0.5s > 门槛。
    original = hub_mod.STABLE_SECONDS
    hub_mod.STABLE_SECONDS = 0.3
    try:
        attempts2, hub2 = await reconnect_case(hold_seconds=0.5, run_seconds=4.0)
    finally:
        hub_mod.STABLE_SECONDS = original

    check_true("稳定后断开：确实重连了", len(attempts2) >= 2, f"{len(attempts2)} 次")
    if len(attempts2) >= 2:
        gaps = [b - a for a, b in zip(attempts2, attempts2[1:])]
        biggest = max(gaps)
        # 每次 = 保持 0.5s + 重连等待。稳定连接的重连等待应该是 1s，
        # 所以间隔约 1.5s。如果退避在涨，第二个间隔就会到 2.5s 以上。
        check_true(
            "稳定后断开：重连间隔没有增长（说明退避被重置了）",
            biggest < 2.2,
            f"间隔={[round(g, 2) for g in gaps]}",
        )
    check("稳定后断开：没有假装自己是连着的", hub2.connected, False)

    print()
    if failures:
        print(f"❌ {len(failures)}/{total} 条失败：")
        for name in failures:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
