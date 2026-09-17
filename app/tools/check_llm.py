"""模型自检：一条命令验证「你的 LLM 配置到底能不能用」。

    python -m app.tools.check_llm
    python -m app.tools.check_llm --model google/gemini-2.5-flash
    python -m app.tools.check_llm --model openrouter/anthropic/claude-sonnet-4

为什么需要它：这条工作流最大的风险不是「模型答错」，而是「模型静默不可用」——
key 过期、余额耗尽、模型名写错、base URL 被反代改了，用户却不知道，
直到某天发现漏了一堆通知。所以这里把配置、连通性、JSON 模式、
以及**真实抽取链路**逐层验一遍，任何一层不通就非 0 退出。

只发 3 次请求，成本极低（几百 token）。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime

from app.config import get_settings
from app.llm import LLMError, acompletion, resolve
from app.pipeline.extract import PROMPT_VER, extract_with_llm
from app.utils import iso_local

# 两条真实语气的样例：一条时间明确，一条故意没有明确时间（应该给 due_at=null）
SAMPLES = [
    "本周五19:00在教三201开班会，请全体同学准时参加，不要迟到。",
    "关于奖学金评定，后续安排请关注群通知。",
]


def _show_model(model: str) -> tuple[str, str, str] | None:
    """打印解析结果（**不打印 key**），返回 (provider, kind, base) 供后续使用。"""
    try:
        provider, name = resolve(model)
    except LLMError as exc:
        print(f"  ✗ 模型名无法解析：{exc}")
        return None

    key = provider.api_key()
    if provider.requires_key:
        key_state = "已配置" if key else f"缺失（需要 {' 或 '.join(provider.key_envs)}）"
    else:
        key_state = "不需要"

    print(f"  provider = {provider.name}")
    print(f"  协议     = {provider.kind}（{'OpenAI 格式' if provider.kind == 'openai' else 'Google 原生格式'}）")
    print(f"  模型名   = {name}")
    print(f"  base URL = {provider.resolved_base_url()}")
    print(f"  API key  = {key_state}")
    return provider.name, provider.kind, provider.resolved_base_url()


async def _probe(model: str, *, label: str, json_mode: bool) -> bool:
    messages = [
        {"role": "system", "content": "你是一个严谨的助手。"},
        {"role": "user", "content": '只回复这个 JSON，不要任何解释：{"ok": true}' if json_mode else "回复两个字：收到"},
    ]
    started = time.monotonic()
    try:
        result = await acompletion(
            model=model, messages=messages, temperature=0.0, timeout=30.0, json_mode=json_mode
        )
    except LLMError as exc:
        print(f"  ✗ {label}失败：{exc}")
        return False

    elapsed = time.monotonic() - started
    preview = result.text.strip().replace("\n", " ")[:80]
    print(f"  ✓ {label}通过（{elapsed:.1f}s，{result.total_tokens} tokens）→ {preview!r}")
    if not result.text.strip():
        print("    ⚠️ 返回了空文本 —— 模型或反代可能有问题")
        return False
    return True


async def _probe_extraction(model: str) -> bool:
    """跑真实的抽取链路（含 evidence 硬约束、due_at 合理性检查）。"""
    ts = int(datetime(2026, 9, 16, 15, 0).timestamp() * 1000)  # 周三 15:00
    ok = True
    for i, content in enumerate(SAMPLES):
        raw = {
            "ts": ts,
            "content": content,
            "group_name": "自检样例群",
            "sender_name": "自检",
            "message_id": f"selfcheck-{i}",
        }
        started = time.monotonic()
        try:
            out = await extract_with_llm(raw, [])
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ 样例 {i + 1} 抽取失败：{exc}")
            ok = False
            continue

        result = out["result"]
        elapsed = time.monotonic() - started
        if result is None:
            # 模型判定"这不是通知"，或 evidence 为空被丢弃 —— 两种都算合理结果
            print(f"  ✓ 样例 {i + 1} 未建条（判为非通知 / evidence 为空），{elapsed:.1f}s")
            continue

        due = result["due_at"]
        due_show = iso_local(due) if due else f"未识别（due_text={result['due_text']!r}）"
        print(f"  ✓ 样例 {i + 1} 建条 {elapsed:.1f}s")
        print(f"      标题 = {result['title']}")
        print(f"      截止 = {due_show}")
        print(f"      地点 = {result['location'] or '未提到'}")
        print(f"      置信 = {result['due_confidence']}（冲突={result['conflict']}）")
        print(f"      证据 = {result['evidence'][:70]!r}")

        if i == 0 and due is None:
            print("      ⚠️ 这条样例有明确的「本周五19:00」，却没抽到 due_at")
            ok = False

    return ok


async def main_async(args: argparse.Namespace) -> int:
    settings = get_settings()
    model = args.model or settings.llm_primary_model
    secondary = "" if args.model else settings.llm_secondary_model

    print(f"配置文件 prompt 版本：{PROMPT_VER}")
    print(f"当前 EXTRACTOR = {settings.extractor}（cross_check={'开' if settings.cross_check_enabled else '关'}）")
    print()

    print(f"■ 主模型 {model}")
    info = _show_model(model)
    if info is None:
        return 2
    print()

    failures = 0
    print("■ 连通性")
    if not await _probe(model, label="基础对话", json_mode=False):
        failures += 1
    if not await _probe(model, label="JSON 模式", json_mode=True):
        # JSON 模式不支持时生产链路会自动退回普通模式，所以只提示、不算致命
        print("    （生产链路第一次会试 JSON 模式，失败后自动退回普通模式，不影响可用性）")
    print()

    print("■ 真实抽取链路")
    if not await _probe_extraction(model):
        failures += 1
    print()

    if secondary:
        print(f"■ 交叉验证模型 {secondary}")
        if _show_model(secondary) is not None:
            if not await _probe(secondary, label="基础对话", json_mode=False):
                failures += 1
        else:
            failures += 1
        print()

    if failures:
        print(f"❌ 有 {failures} 项没过。上面的报错里已经写明了原因。")
        return 1
    print("✅ 模型配置可用。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="验证 LLM 配置能不能用")
    parser.add_argument("--model", default="", help="临时指定主模型，如 google/gemini-2.5-flash")
    args = parser.parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
