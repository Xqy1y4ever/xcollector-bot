"""中文相对时间解析。

这一层是纯确定性的，同时服务三个用途：
  1. rule 抽取器直接用它
  2. 给 LLM 提供锚点信息（消息发送时间）
  3. 校验 LLM 返回的时间是否离谱

设计要点（对应 docs/design.md §6.1）：
  - 锚点是**消息发送时间**，不是当前时间。否则"下周三"会被解析到错误的一周。
  - 解析不出来就返回 None，绝不猜。
  - due_text 只保留**时间短语本身**（"下周三前"），整句话留给 evidence。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ..config import get_settings

WEEKDAY = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6}

PAT_FULL = re.compile(r"(\d{4})\s*[-/年.]\s*(\d{1,2})\s*[-/月.]\s*(\d{1,2})\s*[日号]?")
PAT_MD = re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]")
PAT_WEEK = re.compile(r"(下下|下|本|这)?\s*(?:周|星期|礼拜)\s*([一二三四五六日天])")
PAT_DAY = re.compile(r"(大后天|后天|明天|明晚|今晚|今天)")
PAT_AFTER = re.compile(r"(\d{1,3})\s*(天|日|小时|周)\s*(?:后|以后|之内|内)")

PAT_HHMM = re.compile(r"(\d{1,2})\s*[:：]\s*([0-5]\d)")
PAT_CN_HOUR = re.compile(r"(\d{1,2})\s*[点时](?:\s*(半)|\s*([0-5]?\d)\s*分)?")
PAT_CN_PERIOD = re.compile(r"(中午|上午|下午|晚上|傍晚|凌晨|早上|清晨)")

CN_PERIOD_CLOCK = {
    "中午": (12, 0),
    "上午": (9, 0),
    "下午": (15, 0),
    "晚上": (20, 0),
    "傍晚": (18, 0),
    "凌晨": (1, 0),
    "早上": (8, 0),
    "清晨": (7, 0),
}

# 时间短语的"尾巴"：可选的时段词 + 可选的具体时刻 + 可选的前后缀
CONT_TAIL = re.compile(
    r"(?:\s*(?:中午|上午|下午|晚上|傍晚|凌晨|早上|清晨))?"
    r"(?:\s*(?:\d{1,2}\s*[点时](?:\s*半|\s*[0-5]?\d\s*分)?|\d{1,2}\s*[:：]\s*[0-5]\d))?"
    r"(?:\s*(?:之前|以前|以内|之内|截止|前|内))?"
)

DEFAULT_CLOCK = (23, 59)  # 只说日期不说时间 → 当天最后一分钟
SENTENCE_SPLIT = re.compile(r"[。！？!?\n；;，,]")


@dataclass
class DueGuess:
    due_at: int | None
    due_text: str  # 原文时间短语，如「下周三前」
    sentence: str  # 包含该短语的那句话，用于 evidence
    confidence: float


def _tz():
    return get_settings().tz


def _local(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(_tz())


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def sentence_around(text: str, start: int, end: int, pad: int = 40) -> str:
    """取包含 [start, end) 的那句话，用于 evidence。"""
    left = 0
    for m in SENTENCE_SPLIT.finditer(text):
        if m.end() <= start:
            left = m.end()
        else:
            break
    right = len(text)
    for m in SENTENCE_SPLIT.finditer(text):
        if m.start() >= end:
            right = m.start()
            break
    frag = text[left:right].strip()
    if not frag:
        frag = text[max(0, start - pad) : min(len(text), end + pad)].strip()
    return frag


def _phrase(text: str, m: re.Match) -> str:
    """从匹配处向后吸收时段词/时刻/前后缀，得到完整的时间短语。"""
    tail = CONT_TAIL.match(text, m.end())
    end = tail.end() if tail else m.end()
    return text[m.start() : end].strip()


def _clock_from(text: str, window_start: int, window_end: int) -> tuple[int, int, float]:
    """在 [window_start, window_end) 里找时间点，返回 (时, 分, 置信度加成)。

    时段词（下午/晚上/凌晨…）和具体时刻（3点、19:00）要**合并**判断，
    否则「下午3点半」会被读成凌晨 3:30 —— 这类错误会让用户错过截止时间。
    """
    clock: tuple[int, int, float] | None = None
    for m in PAT_HHMM.finditer(text):
        if not (window_start <= m.start() < window_end):
            continue
        hh, mm = int(m.group(1)), int(m.group(2))
        if hh == 24:
            clock = (23, 59, 0.1)  # "24:00" 即当天最后一刻
        elif 0 <= hh <= 23:
            clock = (hh, mm, 0.1)
        break

    if clock is None:
        for m in PAT_CN_HOUR.finditer(text):
            if not (window_start <= m.start() < window_end):
                continue
            hh = int(m.group(1))
            if hh == 24:
                clock = (23, 59, 0.1)
            elif 0 <= hh <= 23:
                mm = 30 if m.group(2) else (int(m.group(3)) if m.group(3) else 0)
                clock = (hh, mm, 0.1)
            break

    period: str | None = None
    for m in PAT_CN_PERIOD.finditer(text):
        if window_start <= m.start() < window_end:
            period = m.group(1)
            break

    if clock is not None:
        hh, mm, bonus = clock
        if period == "中午":
            if 0 < hh < 12:
                hh += 12
        elif period in ("下午", "晚上", "傍晚"):
            if 0 < hh < 12:
                hh += 12
        elif period in ("凌晨", "早上", "清晨", "上午"):
            if hh == 12:
                hh = 0
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return (hh, mm, bonus)

    if period is not None:
        hh, mm = CN_PERIOD_CLOCK[period]
        return (hh, mm, 0.05)

    return (DEFAULT_CLOCK[0], DEFAULT_CLOCK[1], 0.0)


def _weekday_target(base: datetime, prefix: str, wd: int) -> datetime:
    """计算「周X / 本周X / 下周X / 下下周X」对应的日期。

    以周一为一周之始。注意不能用 (wd - base.weekday()) % 7 + 7 —— 那会
    把「下周一」算到下下周去。
    """
    monday = base - timedelta(days=base.weekday())
    if prefix == "下下":
        return monday + timedelta(days=14 + wd)
    if prefix == "下":
        return monday + timedelta(days=7 + wd)
    if prefix in ("本", "这"):
        return monday + timedelta(days=wd)
    # 裸「周X」：默认本周；若已过去则顺延到下周
    target = monday + timedelta(days=wd)
    if target.date() < base.date():
        target += timedelta(days=7)
    return target


def parse_due(text: str, anchor_ms: int) -> DueGuess | None:
    """从文本里解析截止时间。返回 None 表示没能可靠解析。"""
    if not text:
        return None
    anchor = _local(anchor_ms) if anchor_ms else None
    if anchor is None:
        return None
    base = anchor.replace(hour=0, minute=0, second=0, microsecond=0)

    # ---- 1. 完整日期 YYYY-MM-DD ----
    m = PAT_FULL.search(text)
    if m:
        try:
            target = base.replace(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))
        except ValueError:
            target = None
        if target:
            hh, mm, bonus = _clock_from(text, m.end(), min(len(text), m.end() + 40))
            target = target.replace(hour=hh, minute=mm)
            return DueGuess(
                _to_ms(target), _phrase(text, m), sentence_around(text, m.start(), m.end()),
                round(min(0.99, 0.9 + bonus), 2),
            )

    # ---- 2. M月D日 ----
    m = PAT_MD.search(text)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        try:
            target = base.replace(month=month, day=day)
        except ValueError:
            target = None
        if target:
            if (base - target).days > 30:  # 已过去一个多月 → 应指明年
                try:
                    target = target.replace(year=base.year + 1)
                except ValueError:
                    pass
            hh, mm, bonus = _clock_from(text, m.end(), min(len(text), m.end() + 40))
            target = target.replace(hour=hh, minute=mm)
            return DueGuess(
                _to_ms(target), _phrase(text, m), sentence_around(text, m.start(), m.end()),
                round(min(0.95, 0.85 + bonus), 2),
            )

    # ---- 3. 周X / 下周X / 下下周X ----
    m = PAT_WEEK.search(text)
    if m:
        target = _weekday_target(base, m.group(1) or "", WEEKDAY[m.group(2)])
        hh, mm, bonus = _clock_from(text, m.end(), min(len(text), m.end() + 40))
        target = target.replace(hour=hh, minute=mm)
        return DueGuess(
            _to_ms(target), _phrase(text, m), sentence_around(text, m.start(), m.end()),
            round(0.7 + bonus, 2),
        )

    # ---- 4. 今天/明天/后天 ----
    m = PAT_DAY.search(text)
    if m:
        word = m.group(1)
        offset = {"今天": 0, "今晚": 0, "明天": 1, "明晚": 1, "后天": 2, "大后天": 3}[word]
        target = base + timedelta(days=offset)
        if word.endswith("晚"):
            target = target.replace(hour=20, minute=0)
            bonus = 0.05
        else:
            hh, mm, bonus = _clock_from(text, m.end(), min(len(text), m.end() + 40))
            target = target.replace(hour=hh, minute=mm)
        return DueGuess(
            _to_ms(target), _phrase(text, m), sentence_around(text, m.start(), m.end()),
            round(0.8 + bonus, 2),
        )

    # ---- 5. X天后 / X小时内 ----
    m = PAT_AFTER.search(text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit in ("天", "日"):
            target = base + timedelta(days=n)
        elif unit == "周":
            target = base + timedelta(weeks=n)
        else:
            target = anchor + timedelta(hours=n)
        if unit in ("天", "日", "周"):
            hh, mm, bonus = _clock_from(text, m.end(), min(len(text), m.end() + 40))
            target = target.replace(hour=hh, minute=mm)
        else:
            bonus = 0.0
        return DueGuess(
            _to_ms(target), _phrase(text, m), sentence_around(text, m.start(), m.end()),
            round(0.75 + bonus, 2),
        )

    return None
