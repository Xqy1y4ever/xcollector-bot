"""把一条假的 OneBot 群消息**直接喂给 bot 的接收链路**（不需要 NapCat）。

    python -m app.tools.feed_event
    python -m app.tools.feed_event --count 3 --wait 4
    python -m app.tools.feed_event --backend-url http://127.0.0.1:9000

和 send_test 的区别：
  - send_test 测的是"归一化 + 直接写后端"；
  - **feed_event 测的是 bot 真正收到消息时走的那条路**：
    handle_event → 归一化 → 写前日志 → 队列 → 附件/抽取 → 建通知。

  所以它能回答"NapCat 没连上、后端也没连上时，收到消息这条链路会不会崩"这个问题 ——
  这是拆进程之后最容易出问题、又最难在没环境的机器上复现的一环。

hub 用 FakeHub 顶替（没有真实 WS），但**其余全是真代码**。

注意：这里**不会**连真的 NapCat。要跑通它请把后端指向假后端：
    python -m app.tools.fake_backend --port 9000
    python -m app.tools.feed_event --backend-url http://127.0.0.1:9000 --count 1
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import sys

from ..config import Settings, get_settings
from ..logging_setup import setup_logging
from .send_test import FAKE_EVENT, FakeHub

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass


def build_events(count: int) -> list[dict]:
    events = []
    for i in range(count):
        event = copy.deepcopy(FAKE_EVENT)
        event["message_id"] = 70000 + i
        event["time"] = FAKE_EVENT["time"] + i
        events.append(event)
    return events


async def run(args: argparse.Namespace) -> int:
    # 注意这里**不**导入 BotRuntime 的默认 hub 循环：FakeHub 顶替之后
    # 不会有人去连 127.0.0.1:3001，日志里也就不会有重连噪音。
    from ..main import BotRuntime

    settings = Settings(
        backend_base_url=args.backend_url,
        backend_timeout=args.timeout,
        backend_max_retries=args.retries,
        extractor=args.extractor,
        media_download_enabled=not args.no_media,
    )
    runtime = BotRuntime(settings)
    runtime.hub = FakeHub()  # type: ignore[assignment]

    # 只启动流水线的后台 worker，不启动 OneBot / digest / watchdog：
    # 这个工具的目的是验证"一条消息进来之后发生了什么"。
    await runtime.pipeline.start()
    try:
        for event in build_events(args.count):
            # 这就是 hub._safe_handle 真正会调用的那个入口
            await runtime.handle_event(event)
        # 等写前日志 + 队列里的重活（附件/抽取/建通知）跑完
        await asyncio.sleep(args.wait)
        print("=== /api/status（groups 应含刚才那个群）===")
        print(json.dumps(await runtime.status_payload(), ensure_ascii=False, indent=2))
    finally:
        await runtime.pipeline.stop()
        await runtime.backend.close()

    print(f"[OK] 接收链路没有崩：喂了 {args.count} 条，NapCat 始终未连接")
    return 0


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    parser = argparse.ArgumentParser(prog="python -m app.tools.feed_event")
    parser.add_argument("--backend-url", default=None)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--wait", type=float, default=4.0, help="等流水线跑完的秒数")
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--retries", type=int, default=None)
    parser.add_argument(
        "--extractor", default="rule", help="rule/llm/both（默认 rule：不需要 litellm）"
    )
    parser.add_argument("--no-media", action="store_true", help="不下载附件")
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.backend_url is None:
        args.backend_url = settings.backend_base_url
    if args.timeout is None:
        args.timeout = settings.backend_timeout
    if args.retries is None:
        args.retries = settings.backend_max_retries

    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
