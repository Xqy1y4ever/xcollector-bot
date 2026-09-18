"""时间解析回归测试。

    python -m tests.check_timeparse

时间解析是「DDL 不能错」这条要求的关键路径，任何改动都必须先过这里。
锚点固定为 2026-09-16（周三）15:00，避免测试随运行日期漂移。
"""

from __future__ import annotations

import sys
from datetime import datetime

from app.config import get_settings
from app.pipeline.timeparse import parse_due

TZ = get_settings().tz
ANCHOR = int(datetime(2026, 9, 16, 15, 0, tzinfo=TZ).timestamp() * 1000)

# (输入, 期望的本地时间 或 None, 期望的 due_text 或 None)
CASES: list[tuple[str, datetime | None, str | None]] = [
    # 周表达：最容易算错的一组
    ("大家下周三前把军训心得交到班长那里", datetime(2026, 9, 23, 23, 59), "下周三前"),
    ("下周一之前必须完成", datetime(2026, 9, 21, 23, 59), "下周一之前"),
    ("下下周一交", datetime(2026, 9, 28, 23, 59), "下下周一"),
    ("本周五19:00在教三201开班会", datetime(2026, 9, 18, 19, 0), "本周五19:00"),
    ("周三前交", datetime(2026, 9, 16, 23, 59), "周三前"),      # 今天就是周三
    ("周四前交", datetime(2026, 9, 17, 23, 59), "周四前"),
    ("周一前交", datetime(2026, 9, 21, 23, 59), "周一前"),      # 已过去 → 顺延
    # 「这周天/这周日」和「周末」（v3 新增的两种说法）
    ("这周天前交材料", datetime(2026, 9, 20, 23, 59), "这周天前"),
    ("这周日交", datetime(2026, 9, 20, 23, 59), "这周日"),
    ("本周末前交", datetime(2026, 9, 20, 23, 59), "本周末前"),
    ("下周末前交", datetime(2026, 9, 27, 23, 59), "下周末前"),
    ("周末前交材料", datetime(2026, 9, 20, 23, 59), "周末前"),
    # 月底 / 本月内 / 下个月底（v3 新增）
    ("月底前交材料", datetime(2026, 9, 30, 23, 59), "月底前"),
    ("本月底前交", datetime(2026, 9, 30, 23, 59), "本月底前"),
    ("本月内完成", datetime(2026, 9, 30, 23, 59), "本月内"),
    ("下个月底前交", datetime(2026, 10, 31, 23, 59), "下个月底前"),
    # 绝对日期
    ("9月20日24:00前完成选课确认", datetime(2026, 9, 20, 23, 59), "9月20日24:00前"),
    ("9月20日交", datetime(2026, 9, 20, 23, 59), "9月20日"),
    ("2026-10-01前提交", datetime(2026, 10, 1, 23, 59), "2026-10-01前"),
    # 相对日
    ("明天中午12点前把体检表交到学工办", datetime(2026, 9, 17, 12, 0), "明天中午12点前"),
    ("后天下午3点半开会", datetime(2026, 9, 18, 15, 30), "后天下午3点半"),
    ("今天晚上8点交", datetime(2026, 9, 16, 20, 0), "今天晚上8点"),
    # 相对量
    ("请3天内报给我", datetime(2026, 9, 19, 23, 59), "3天内"),
    ("2小时后关闭", datetime(2026, 9, 16, 17, 0), "2小时后"),
    # 解析不出来 → 必须返回 None，绝不猜
    ("尽快完成", None, None),
    ("后续安排请关注群通知", None, None),
    ("收到", None, None),
    # 「周末/月底」不带截止词时**不是**截止时间（"周末一起吃饭"是闲聊）
    ("周末一起吃饭", None, None),
    ("月底我们去旅游", None, None),
    ("下周见", None, None),
]


def main() -> int:
    failures = 0
    for text, want_dt, want_text in CASES:
        guess = parse_due(text, ANCHOR)
        if want_dt is None:
            if guess is not None:
                print(f"FAIL  {text!r}\n      期望无法解析，实际得到 {guess.due_at} / {guess.due_text!r}")
                failures += 1
            else:
                print(f"ok    {text!r} -> 正确判定为无法解析")
            continue

        if guess is None:
            print(f"FAIL  {text!r}\n      期望 {want_dt}，实际无法解析")
            failures += 1
            continue

        got = datetime.fromtimestamp(guess.due_at / 1000, tz=TZ).replace(tzinfo=None, microsecond=0)
        ok_time = got == want_dt
        ok_text = guess.due_text == want_text

        if ok_time and ok_text:
            print(f"ok    {text!r} -> {got:%Y-%m-%d %H:%M}  due_text={guess.due_text!r}")
        else:
            failures += 1
            print(f"FAIL  {text!r}")
            if not ok_time:
                print(f"      时间：期望 {want_dt}，实际 {got}")
            if not ok_text:
                print(f"      due_text：期望 {want_text!r}，实际 {guess.due_text!r}")

    print()
    if failures:
        print(f"❌ {failures}/{len(CASES)} 条失败")
        return 1
    print(f"✅ {len(CASES)} 条全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
