"""端到端自检：造一条假的 OneBot 群消息，走完整条归一化链路，再按契约写进后端。

    python -m app.tools.send_test                 # 归一化 + 写 BACKEND_BASE_URL
    python -m app.tools.send_test --print-only    # 只看归一化结果，不发网络请求
    python -m app.tools.send_test --forward-fail  # 模拟合并转发展开失败（看降级文案）
    python -m app.tools.send_test --count 5       # 连发 5 条
    python -m app.tools.send_test --to-self       # 向 bot 自己的 /api/send/private 发一条假消息
    python -m app.tools.send_test --backend-url http://127.0.0.1:9000

它不需要 NapCat：`FakeHub` 顶替 hub，把 get_forward_msg / get_group_info
换成固定数据，于是合并转发递归展开、群名缓存这些逻辑都能被真实执行到。

写后端那一步现在走的是**真流水线**（pipeline.runner.ingest_message）：
写前日志 → 附件 → 抽取 → 建通知 → 统计。
默认 `--extractor rule`，这样不发任何模型请求就能把链路跑通。
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import sys
import time

import httpx

from ..backend_client import BackendClient
from ..config import Settings, get_settings
from ..logging_setup import setup_logging
from ..normalize import MessageNormalizer
from ..pipeline.runner import ingest_message

# Windows 控制台默认 GBK，print 中文/emoji 会 UnicodeEncodeError
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

# 一条"典型"的群通知：@全体成员 + 正文 + 图片 + 合并转发 + 回复。
# 刻意做全，是因为这些段类型正是接入期最容易出问题的地方。
FAKE_EVENT: dict = {
    "post_type": "message",
    "message_type": "group",
    "sub_type": "normal",
    "message_id": 12345,
    "group_id": 123456789,
    "user_id": 10001,
    "self_id": 999999,
    "time": 1757692800,  # 秒；归一化后应变成 1757692800000
    "sender": {
        "user_id": 10001,
        "nickname": "小李",
        "card": "张老师",  # card 必须赢过 nickname
        "role": "admin",
    },
    "message": [
        {"type": "at", "data": {"qq": "all"}},
        {"type": "text", "data": {"text": " 大家下周三前把军训心得交到班长那里，不少于800字。"}},
        {
            "type": "image",
            "data": {"url": "https://example.com/x.jpg", "file": "x.jpg", "size": "12345"},
        },
        {
            "type": "reply",
            "data": {"id": "90001"},
        },
        {"type": "forward", "data": {"id": "fwd-001"}},
    ],
}

# 合并转发的内容：故意套一层 fwd-002，用来验证"递归"确实发生了
FORWARD_FIXTURES: dict[str, list[dict]] = {
    "fwd-001": [
        {
            "sender": {"nickname": "张三"},
            "message": [{"type": "text", "data": {"text": "军训心得下周三前交，注意格式"}}],
        },
        {
            "sender": {"nickname": "李四"},
            "message": [
                {"type": "at", "data": {"qq": "all"}},
                {"type": "text", "data": {"text": "收到"}},
                {"type": "forward", "data": {"id": "fwd-002"}},
            ],
        },
    ],
    "fwd-002": [
        {
            "sender": {"nickname": "王五"},
            "message": [{"type": "text", "data": {"text": "更深一层：附件在这个链接里"}}],
        }
    ],
}

GROUP_INFO = {
    "group_id": 123456789,
    "group_name": "示例通知群",
    "member_count": 200,
}


class FakeHub:
    """顶替 OneBotHub 的最小实现。

    只实现归一化真正会调用的两个动作，所以它同时也是一份
    "归一化到底依赖 OneBot 的哪些能力"的文档。
    """

    def __init__(self, fail_forward: bool = False, fail_group_info: bool = False):
        self.fail_forward = fail_forward
        self.fail_group_info = fail_group_info
        self.calls: list[str] = []
        # 让 FakeHub 也能撑起运行时里"连接状态"的读取（没有真实连接）
        self.connected = False

    async def get_forward_msg(self, forward_id: str) -> list[dict]:
        self.calls.append(f"get_forward_msg({forward_id})")
        if self.fail_forward:
            raise TimeoutError("模拟 get_forward_msg 超时")
        return copy.deepcopy(FORWARD_FIXTURES.get(forward_id, []))

    async def get_group_info(self, group_id: str) -> dict:
        self.calls.append(f"get_group_info({group_id})")
        if self.fail_group_info:
            raise RuntimeError("模拟 get_group_info 失败")
        return dict(GROUP_INFO)

    def status(self) -> dict:
        """让 FakeHub 也能撑起 /api/status 的形状（没有真实连接）。"""
        return {
            "connected": False,
            "mode": "fake",
            "target": "(in-process fake hub)",
            "last_event_at": None,
            "reconnect_count": 0,
            "last_error": None,
        }


def build_events(count: int) -> list[dict]:
    events = []
    for i in range(count):
        event = copy.deepcopy(FAKE_EVENT)
        event["message_id"] = 12345 + i
        event["time"] = FAKE_EVENT["time"] + i
        events.append(event)
    return events


async def run_ingest(args: argparse.Namespace) -> int:
    settings = Settings(
        backend_base_url=args.backend_url,
        backend_timeout=args.timeout,
        backend_max_retries=args.retries,
        extractor=args.extractor,
        media_download_enabled=not args.no_media,
    )
    hub = FakeHub(fail_forward=args.forward_fail)
    normalizer = MessageNormalizer(settings)

    messages: list = []
    for event in build_events(args.count):
        msg = await normalizer.normalize(event, hub)
        if msg is None:
            print("!! 归一化返回 None，事件被忽略了")
            return 1
        messages.append(msg)

    print("=== 归一化结果（第 1 条）===")
    print(json.dumps(messages[0].to_dict(), ensure_ascii=False, indent=2))
    print(f"=== 共 {len(messages)} 条，FakeHub 调用：{hub.calls} ===")

    if args.print_only:
        return 0

    client = BackendClient(settings)
    started = time.monotonic()
    outcomes: list[str] = []
    try:
        for msg in messages:
            outcomes.append(await ingest_message(msg, client, settings=settings))
    finally:
        await client.close()
    elapsed = time.monotonic() - started

    print(f"=== 处理结果：{outcomes}（{elapsed:.2f}s）===")
    if "error" in outcomes:
        print(
            f"[FAIL] 有消息没能写进 {settings.backend_base}；"
            "上面应该有 WARNING/ERROR 日志（退避 1s/2s/4s）"
        )
        return 2
    print(f"[OK] 已按契约写入 {settings.backend_base}：{outcomes}")
    return 0


async def run_to_self(args: argparse.Namespace) -> int:
    """向 bot 自己的 /api/send/private 发一条假消息。

    这条路走通说明 bot 的 HTTP 层是活的；NapCat 没连时应当**返回 200 +
    {"ok": false, "error": ...}**，而不是 500 —— 这是后端能区分
    "调用失败 / 发送失败"的前提。
    """
    settings = get_settings()
    url = args.bot_url.rstrip("/") + "/api/send/private"
    headers = {}
    if args.bot_token:
        headers["Authorization"] = f"Bearer {args.bot_token}"

    body = {"user_id": args.to_self_user, "message": args.to_self_message}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, json=body, headers=headers)

    print(f"POST {url} -> HTTP {resp.status_code}")
    try:
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(resp.text[:500])

    if resp.status_code != 200:
        print("[FAIL] 期望 200（即使发送失败也必须是 200 + ok=false）")
        return 1
    payload = resp.json()
    if payload.get("ok") is False and payload.get("error"):
        print("[OK] 未连接 NapCat 时正确返回了 ok=false + error（不是 500）")
        return 0
    print("[OK] 请求已被接受（ok=true 说明 NapCat 已连接，消息真的发出去了）")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.tools.send_test",
        description="Xcollector bot 端到端自检工具（不需要 NapCat / backend 也能跑一部分）",
    )
    parser.add_argument("--backend-url", default=None, help="覆盖 BACKEND_BASE_URL")
    parser.add_argument("--count", type=int, default=1, help="连发几条（默认 1）")
    parser.add_argument("--timeout", type=float, default=None, help="覆盖 BACKEND_TIMEOUT")
    parser.add_argument("--retries", type=int, default=None, help="覆盖 BACKEND_MAX_RETRIES")
    parser.add_argument("--print-only", action="store_true", help="只打印归一化结果，不发请求")
    parser.add_argument(
        "--forward-fail", action="store_true", help="模拟合并转发展开失败，验证降级文案"
    )
    parser.add_argument(
        "--extractor", default="rule", help="rule/llm/both（默认 rule：不调用模型）"
    )
    parser.add_argument("--no-media", action="store_true", help="不下载附件")
    parser.add_argument("--to-self", action="store_true", help="改为向 bot 的 /api/send/private 发消息")
    parser.add_argument("--bot-url", default=None, help="bot 的地址（默认读配置）")
    parser.add_argument("--bot-token", default=None, help="bot 的 BOT_API_TOKEN")
    parser.add_argument("--to-self-user", default="10001", help="假消息的接收 QQ 号")
    parser.add_argument(
        "--to-self-message",
        default="[自检] 这是一条来自 send_test 的假消息",
        help="假消息内容",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)

    settings = get_settings()
    if args.backend_url is None:
        args.backend_url = settings.backend_base_url
    if args.timeout is None:
        args.timeout = settings.backend_timeout
    if args.retries is None:
        args.retries = settings.backend_max_retries
    if args.bot_url is None:
        args.bot_url = f"http://{settings.bot_listen_host}:{settings.bot_listen_port}"
    if args.bot_token is None:
        args.bot_token = settings.bot_api_token

    if args.to_self:
        return asyncio.run(run_to_self(args))
    return asyncio.run(run_ingest(args))


if __name__ == "__main__":
    sys.exit(main())
