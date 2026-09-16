"""地点抽取回归测试。

    python -m tests.check_location

规则抽取地点**不可能做得准**（中文场所名千变万化），所以设计目标是
「宁可返回 None，也不要返回一个错的地点」：
  - 认得出「地点：xxx」和「在/到 + 场所词」这两类明确形态
  - 认不出就交给 LLM，绝不硬猜

因此下面有相当一部分用例期望的结果是 None —— 漏抽和错抽相比，
错抽的危害大得多（用户会照着错的地点跑一趟）。
"""

from __future__ import annotations

import sys

from app.pipeline.rule_extract import rule_extract, rule_location

# (原文, 期望地点)
LOCATION_CASES: list[tuple[str, str | None]] = [
    # ---- 应当认出来 ----
    ("本周五19:00在教三201开班会，请全体同学准时参加。", "教三201"),
    ("明天中午12点前把体检表交到学工办，过时不候。", "学工办"),
    ("地点：体育馆", "体育馆"),
    ("请于9月20日前到图书馆集合", "图书馆"),
    ("考试地点：教二301", "教二301"),
    ("下午3点在办公楼会议室开会", "办公楼会议室"),

    # ---- 必须返回 None，不能硬猜 ----
    ("大家下周三前把军训心得交到班长那里，不少于800字。", None),  # 「班长那里」不是地点
    ("请各位同学于9月20日24:00前完成本学期选课确认。", None),
    ("关于奖学金评定，后续安排请关注群通知。", None),
    ("收到", None),
    ("哈哈哈哈", None),
    ("活动在举办中，请关注后续通知", None),  # 「举办」不能被当成地点
    ("到时会通知大家", None),
    ("请大家在本周内完成", None),
]


def main() -> int:
    failures = 0

    print("=== 地点抽取 ===")
    for text, want in LOCATION_CASES:
        got = rule_location(text)
        if got == want:
            print(f"ok    {text[:34]!r} -> {got!r}")
        else:
            failures += 1
            print(f"FAIL  {text[:34]!r}\n      期望 {want!r}，实际 {got!r}")

    # ---- 整条规则抽取也要带上 location ----
    print("\n=== 规则抽取结果含 location ===")
    result = rule_extract("本周五19:00在教三201开班会", 1757692800000, at_all=True)
    if result is None:
        failures += 1
        print("FAIL  规则抽取返回了 None")
    elif "location" not in result:
        failures += 1
        print(f"FAIL  抽取结果里没有 location 字段：{sorted(result)}")
    elif result["location"] != "教三201":
        failures += 1
        print(f"FAIL  location 期望 '教三201'，实际 {result['location']!r}")
    else:
        print(f"ok    location={result['location']!r}")

    # ---- 没有地点时必须是 None 而不是空串 ----
    result2 = rule_extract("请大家下周三前提交军训心得", 1757692800000)
    if result2 is None:
        failures += 1
        print("FAIL  无地点用例的规则抽取返回了 None")
    elif result2["location"] is not None:
        failures += 1
        print(f"FAIL  无地点时 location 应为 None，实际 {result2['location']!r}")
    else:
        print("ok    原文没有地点时 location=None（而不是空串）")

    print()
    if failures:
        print(f"❌ {failures} 项失败")
        return 1
    print(f"✅ 全部通过（{len(LOCATION_CASES)} 条地点用例）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
