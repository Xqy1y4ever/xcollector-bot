"""LLM 抽取。

这一层的设计目标不是"让 LLM 不出错"（做不到），而是：
  1. 出错必须**可被发现** —— evidence 强制非空，且必须是原文逐字片段
  2. 出错**后果有界** —— 校验失败/超时/异常一律降级，原文照常留在库里
  3. 关键字段**有独立证据** —— 双模型交叉验证，due_at 不一致就标红
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from ..config import get_settings
from ..llm.target import LLMTarget, target_from_settings
from ..utils import iso_local, now_ms, parse_iso_to_ms, to_local

logger = logging.getLogger(__name__)

PROMPT_VER = "llm-v2"  # v2: 新增 location 抽取

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

SYSTEM_PROMPT = """你是一个官方通知抽取器。输入是 QQ 群里发布的官方通知原文。你的任务是判断它是不是一条需要人去做事的通知，并把其中的任务和截止时间抽取出来。

【最重要的规则】
1. 你只能依据原文，不得推测、不得补充原文里没有的信息。
2. evidence 字段必须是从原文中**逐字复制**的一段文字，用来支撑你的判断。如果原文里没有时间信息，evidence 就填支撑"这是一条通知"的那句话。evidence 不能为空，也不能是你自己总结的话。
3. 拿不准的时候，宁可把 due_at 填 null、只在 due_text 里保留原文的时间说法，也**不许猜**一个具体时间。猜错的时间比没有时间危害更大，因为用户会直接相信它。
4. 只有闲聊、回执（"收到""好的""谢谢老师"）、纯表情、广告、纯提问，才算 is_notification=false。凡是让人做事、或告知安排的信息，都算 true。
5. 一条消息里如果有多个截止时间，只抽取最主要的那一个，并在 summary 里说明其他安排。
6. location 必须是原文里**明确写出**的地点（教室、办公室、场馆、校区、线上平台等）。原文没写就填 null，**不许根据常识推测**（比如"交到班长那里"不算地点，"在教三201开会"才算）。

【相对时间】
原文里的"下周三""明天""本周五"等相对说法，一律以**消息发送时间**为锚点计算，不要用今天。
消息发送时间：{send_time}（{weekday}，时区 {tz}）

【输出格式】
只输出一个 JSON 对象，不要任何解释文字，不要 markdown 代码块：
{{
  "is_notification": true 或 false,
  "title": "不超过 20 字的动作标题，祈使句，例如「提交军训心得」",
  "summary": "一到两句话说明要求做什么，不超过 100 字",
  "location": "原文里明确写出的地点，例如「教三201」「学工办」；原文没写就填 null",
  "due_at": "ISO8601 时间，必须带时区偏移，例如 2025-09-12T23:59:00+08:00；无法确定时填 null",
  "due_text": "原文里的时间说法，逐字复制，例如「下周三前」；原文没有就填 null",
  "due_confidence": 0.0 到 1.0 之间的数字，表示你对 due_at 的把握，
  "evidence": "从原文逐字复制的一段文字"
}}

只说明日期、不说具体时间时，截止时间按当天 23:59 计算。"""


class LLMNotification(BaseModel):
    is_notification: bool = False
    title: str = ""
    summary: str = ""
    location: str | None = None
    due_at: str | None = None
    due_text: str | None = None
    due_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence: str = ""


def _strip_code_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _extract_json(text: str) -> dict:
    t = _strip_code_fence(text)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    # 容错：截取第一个 { 到最后一个 }
    start, end = t.find("{"), t.rfind("}")
    if start >= 0 and end > start:
        return json.loads(t[start : end + 1])
    raise ValueError(f"模型未返回合法 JSON：{t[:200]}")


def _build_user_content(raw: dict, images: list[str]) -> Any:
    local_ts = to_local(raw["ts"])
    header = (
        f"群名称：{raw.get('group_name') or raw.get('group_id')}\n"
        f"发送者：{raw.get('sender_name') or raw.get('sender_id')}\n"
        f"发送时间：{iso_local(raw['ts'])}\n"
        f"---\n原文：\n{raw.get('content') or ''}"
    )
    if not images:
        return header

    parts: list[dict] = [{"type": "text", "text": header}]
    for url in images:
        parts.append({"type": "image_url", "image_url": {"url": url}})
    parts.append(
        {"type": "text", "text": "注意：上面的图片也是通知原文的一部分，其中的时间信息同样要抽取。"}
    )
    return parts


async def _call_model(
    target: LLMTarget,
    raw: dict,
    images: list[str],
    *,
    use_json_mode: bool = True,
) -> tuple[LLMNotification, int]:
    """调用一次模型，返回 (解析结果, 消耗 token 数)。失败抛异常。"""
    from app.llm import acompletion  # 延迟导入，保持模块导入链轻量

    settings = get_settings()
    local_ts = to_local(raw["ts"])
    system = SYSTEM_PROMPT.format(
        send_time=local_ts.strftime("%Y-%m-%d %H:%M") if local_ts else "未知",
        weekday=_WEEKDAY_CN[local_ts.weekday()] if local_ts else "未知",
        tz=settings.digest_tz,
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": _build_user_content(raw, images)},
    ]

    result = await acompletion(
        target=target,
        messages=messages,
        temperature=settings.llm_temperature,
        timeout=settings.llm_timeout,
        response_format={"type": "json_object"} if use_json_mode else None,
    )
    data = _extract_json(result.text)
    return LLMNotification(**data), result.total_tokens


async def run_llm(raw: dict, images: list[str], target: LLMTarget) -> tuple[LLMNotification, int]:
    """带重试的模型调用。

    第一次用 JSON 模式；若厂商不支持 response_format 会抛错，则退回普通模式再试。
    """
    settings = get_settings()
    attempts = max(1, settings.llm_max_retries + 1)
    last_error: Exception | None = None

    for i in range(attempts):
        use_json_mode = i == 0
        try:
            return await asyncio.wait_for(
                _call_model(target, raw, images, use_json_mode=use_json_mode),
                timeout=settings.llm_timeout + 10,
            )
        except ValidationError as exc:
            last_error = exc
            logger.warning("模型输出不符合 schema（第 %d 次）：%s", i + 1, exc)
        except Exception as exc:
            last_error = exc
            logger.warning("模型调用失败（第 %d 次，model=%s）：%s", i + 1, model, exc)

    raise RuntimeError(f"模型 {model} 调用失败：{last_error}")


def _looks_like_parse_failure(due_at: int | None, source_ts: int) -> bool:
    """时间明显不合理 → 判定为解析失败而不是真实时间。

    一个"截止时间"比消息本身还早一天以上，几乎一定是模型算错了。
    此时宁可丢掉 due_at、只保留 due_text，也不能让它被当真。
    """
    if due_at is None:
        return False
    if due_at < source_ts - 24 * 3600 * 1000:
        return True
    if due_at > source_ts + 3 * 365 * 24 * 3600 * 1000:
        return True
    return False


def finalize(
    parsed: LLMNotification,
    raw: dict,
    model: str,
    *,
    extractor: str = "llm",
    tokens: int = 0,
) -> dict | None:
    """把模型输出规范化成 notification dict。返回 None 表示这条不该建条。"""
    evidence = re.sub(r"\s+", " ", (parsed.evidence or "")).strip()
    if not parsed.is_notification:
        return None
    if not evidence:
        # evidence 为空 → 无法验证 → 不建条目（硬约束，防幻觉）
        logger.info("evidence 为空，丢弃该抽取结果 raw=%s", raw.get("message_id"))
        return None

    title = (parsed.title or "").strip()[:60] or (raw.get("content") or "")[:40]
    if not title:
        return None

    due_at = parse_iso_to_ms(parsed.due_at)
    if _looks_like_parse_failure(due_at, raw["ts"]):
        logger.info("due_at 明显不合理(%s)，降级为仅有 due_text", parsed.due_at)
        due_at = None
        due_confidence = 0.0
    else:
        due_confidence = float(parsed.due_confidence or 0.0)
        if due_at is None:
            due_confidence = 0.0

    return {
        "title": title,
        "summary": (parsed.summary or "").strip()[:300] or None,
        "location": (parsed.location or "").strip()[:60] or None,
        "due_at": due_at,
        "due_text": (parsed.due_text or "").strip() or None,
        "due_confidence": round(due_confidence, 3),
        "confidence": 0.8,
        "evidence": evidence,
        "extractor": extractor,
        "model": model,
        "prompt_ver": PROMPT_VER,
        "conflict": False,
        "candidates": [{"model": model, "due_at": due_at, "due_text": parsed.due_text}],
        "tokens": tokens,
    }


def cross_check(primary: dict, secondary: dict, secondary_model: str) -> None:
    """就地把交叉验证结果写进 primary。"""
    primary["candidates"].append(
        {
            "model": secondary_model,
            "due_at": secondary["due_at"],
            "due_text": secondary["due_text"],
        }
    )
    a, b = primary["due_at"], secondary["due_at"]

    if a is None and b is None:
        return  # 两个模型都说"没有明确时间"，一致
    if a is None or b is None:
        conflict = True
    else:
        # 允许 1 分钟误差
        conflict = abs(a - b) > 60_000

    if conflict:
        primary["conflict"] = True
        # 有分歧时不能装作有把握
        primary["due_confidence"] = min(primary["due_confidence"], 0.5)
        logger.info(
            "交叉验证冲突：%s=%s vs %s=%s", primary["model"], a, secondary_model, b
        )


async def extract_with_llm(
    raw: dict, images: list[str], *, target: LLMTarget | None = None
) -> dict:
    """跑主模型 +（可选）次模型。抛出异常表示 LLM 这条路整体失败。

    `target` 传了就用它当主模型（自检工具用来临时验证别的端点），
    不传则读配置。
    """
    settings = get_settings()
    primary_target = target or target_from_settings(settings, "primary")
    secondary_target = target_from_settings(settings, "secondary")

    primary, tokens_p = await run_llm(raw, images, primary_target)
    result = finalize(primary, raw, primary_target.label, tokens=tokens_p)

    if result is None:
        return {"result": None, "tokens": tokens_p}

    if settings.cross_check_enabled and target is None:
        try:
            secondary, tokens_s = await run_llm(raw, images, secondary_target)
            tokens_p += tokens_s
            sec = finalize(secondary, raw, secondary_target.label, tokens=tokens_s)
            if sec is not None:
                cross_check(result, sec, secondary_target.label)
            else:
                result["conflict"] = True
                result["candidates"].append(
                    {"model": secondary_target.label, "due_at": None, "due_text": None}
                )
                result["due_confidence"] = min(result["due_confidence"], 0.5)
        except Exception as exc:
            # 次模型失败不影响主结果，但要让用户知道"这次没有交叉验证"
            logger.warning("交叉验证模型失败：%s", exc)
            result["candidates"].append(
                {
                    "model": secondary_target.label,
                    "due_at": None,
                    "due_text": None,
                    "error": str(exc)[:200],
                }
            )

    result["tokens"] = tokens_p
    return {"result": result, "tokens": tokens_p}
