"""抽取层自检：提示词里的日历、判定门槛、以及"模型没算出时间就用规则补"。

    .\\venv\\Scripts\\python.exe -m tests.check_extract

⚠️ 这份文件是从 xcollector-client/tests/check_extract.py **抄过来的**（去掉那里的
isolate_settings()，并把 merge 函数换成 runner 里那个名字）。两个仓库必须用同一套
判据与同一张日历 —— 同一条通知从实时链路（bot）和从聊天记录库（client）进来，
结果不能不一样。改一边就要改另一边。

## 这个文件守的是什么

用户报的两个问题，都落在这一层：

1. **"很多不是通知的内容也被做成任务"** —— 判据太松。三条防线都在这里钉住：
   提示词里明确列出的非通知情形、规则引擎的"只有时间词不算通知"门槛、
   以及"模型说不是通知时，规则只在**有明确时间**时才反着推一条"。
2. **"下周/这周天这种相对时间抽不出来"** —— 这是**日期运算**，对代码是确定性的、
   对模型不是。所以：给模型的提示里直接放一张算好的日历，同时留一条确定性的兜底
   （ill_due_from_rule：模型没给时间、规则算出来了，就用规则的，并且标清来源）。

这份文件**不联网、不调用模型**：只验"我们交给模型的东西"和"模型回来之后我们怎么处理"。
"""

from __future__ import annotations

import sys
from datetime import datetime

from app.config import get_settings
from app.pipeline.extract import (
    LLMNotification,
    PROMPT_VER,
    build_calendar,
    fill_due_from_rule,
    finalize,
    render_system_prompt,
)
from app.pipeline.runner import _merge_rule_disagreement as merge_rule_disagreement
from app.pipeline.rule_extract import rule_extract
from app.pipeline.timeparse import parse_due

TZ = get_settings().tz
# 固定锚点：2026-09-16（周三）15:00 —— 与 tests/check_timeparse.py 同一个锚点
ANCHOR = int(datetime(2026, 9, 16, 15, 0, tzinfo=TZ).timestamp() * 1000)

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


def main() -> int:  # noqa: C901
    # ------------------------------------------------------------------
    print("--- 1. 给模型的日历：算好的日期表，不让模型自己推 ---")
    cal = build_calendar(ANCHOR, "Asia/Shanghai")
    check_true("有本周那一行", "本周：周一 09-14" in cal, cal)
    check_true("有下周那一行", "下周：周一 09-21、周二 09-22、周三 09-23、周四 09-24、"
                              "周五 09-25、周六 09-26、周日 09-27" in cal, cal)
    check_true("有再下周那一行", "再下周：周一 09-28" in cal, cal)
    check_true("有今天/明天/后天", "今天 09-16（周三）、明天 09-17、后天 09-18" in cal, cal)
    check_true("有本月/下月最后一天", "本月最后一天 09-30、下月最后一天 10-31" in cal, cal)
    # 日历里的日期必须和**规则引擎**算出来的完全一致，否则模型和规则会各说一个
    guess = parse_due("下周三前交", ANCHOR)
    check_true(
        "日历里的「下周三」与规则引擎算的是同一天",
        "周三 09-23" in cal
        and datetime.fromtimestamp(guess.due_at / 1000, tz=TZ).strftime("%m-%d") == "09-23",
        f"{guess.due_at}",
    )
    check_true(
        "月末也一致",
        datetime.fromtimestamp(parse_due("月底前交", ANCHOR).due_at / 1000, tz=TZ).strftime("%m-%d")
        == "09-30",
    )
    check_true("发送时间拿不到时不给日历，而是明确说 due_at 必须为 null",
               "无法换算" in build_calendar(None, "Asia/Shanghai"),
               build_calendar(None, "Asia/Shanghai"))

    # ------------------------------------------------------------------
    print("\n--- 2. 提示词：判据、日历、reason 字段，全都真的在里面 ---")
    prompt = render_system_prompt(ANCHOR, "Asia/Shanghai")
    check("提示词版本是 v3", PROMPT_VER, "llm-v3")
    for needle in ("回执与附和", "提问与追问", "已发生事情的回顾", "非官方内容", "只是 @ 某个人",
                   "只有称呼或寒暄", "拿不准就判 false", "把闲聊做成任务比漏掉更烦人"):
        check_true(f"列了这类非通知：{needle}", needle in prompt)
    check_true("有日历（本周那一行）", "本周：周一 09-14" in prompt)
    check_true("把日历挂在「相对时间」一节里", "【相对时间：照下面这张日历换算，不要自己推】" in prompt)
    check_true("要求 reason 字段", '"reason"' in prompt)
    check_true("提示词里的占位符都被替换了",
               all(p not in prompt for p in ("{send_time}", "{weekday}", "{tz}", "{calendar}")),
               "还有没替换的占位符")
    check_true("is_notification=false 时要求写原因", "is_notification=false 时必填" in prompt)

    # ------------------------------------------------------------------
    print("\n--- 3. 规则引擎的门槛：只有时间词不算通知 ---")
    # （这一条直接对应"@张三 明天"「我下周三可能去不了」这类被做成任务的真实误报）
    gate_cases = [
        ("@张三 明天", False),
        ("我下周三可能去不了", False),
        ("下周三我们班聚餐吧", False),
        ("今天上午的会开完了，讲得挺好", False),
        ("收到", False),
        ("各位同学：", False),
        ("周末一起吃饭", False),
        ("刚刚的会议记录我发群里了", True),      # 命中"会议"；有没有时间由下一条断言管
        ("大家下周三前把军训心得交到班长那里", True),
        ("明天上午9点开会", True),
        ("本周五19:00在教三201开班会", True),
        ("月底前把回执交给学工办", True),
        ("本周末前完成线上问卷", True),
        ("这周天下午3点在体育馆集合", True),
        ("请3天内把体检表交到校医院", True),
    ]
    for text, want in gate_cases:
        got = rule_extract(text, ANCHOR) is not None
        check(f"规则引擎：{text[:26]!r} → {'建条' if want else '不建条'}", got, want)

    # ------------------------------------------------------------------
    print("\n--- 4. 模型判「不是通知」时，规则只在**有明确时间**时才反着推 ---")
    model_no = {"model": "m", "due_at": None, "due_text": None, "evidence": "x"}
    with_time = rule_extract("大家下周三前把军训心得交到班长那里", ANCHOR)
    without_time = rule_extract("刚刚的会议记录我发群里了", ANCHOR)
    check_true("规则给了时间 → 反着推一条（标冲突）",
               merge_rule_disagreement(None, with_time, "m") is not None)
    merged = merge_rule_disagreement(None, with_time, "m")
    check("冲突标记为真", merged["conflict"], True)
    check_true("把握被压到 0.5 以下", float(merged["due_confidence"]) <= 0.5, str(merged["due_confidence"]))
    check("只有通知词、没有时间 → 尊重模型", merge_rule_disagreement(None, without_time, "m"), None)
    check("模型给了结果就原样返回", merge_rule_disagreement({"due_at": 1}, with_time, "m"), {"due_at": 1})
    check("两边都没有 → None", merge_rule_disagreement(None, None, "m"), None)

    # ------------------------------------------------------------------
    print("\n--- 5. 兜底：模型判「是通知但没时间」，用规则算出来的时间补上 ---")
    llm_no_due = {
        "title": "提交军训心得",
        "summary": "下周三前交给班长",
        "due_at": None,
        "due_text": None,
        "due_confidence": 0.0,
        "evidence": "大家下周三前把军训心得交到班长那里",
        "extractor": "llm",
        "model": "deepseek-flash",
        "prompt_ver": PROMPT_VER,
        "conflict": False,
        "candidates": [{"model": "deepseek-flash", "due_at": None, "due_text": None}],
        "tokens": 12,
    }
    filled = fill_due_from_rule(dict(llm_no_due), with_time, source_ts=ANCHOR)
    want_due = parse_due("下周三前", ANCHOR).due_at
    check("补上了 due_at", filled["due_at"], want_due)
    check("due_text 用规则的时间短语", filled["due_text"], "下周三前")
    check_true("把握不超过 0.8（不是模型自己算的，不该显得更有把握）",
               0 < float(filled["due_confidence"]) <= 0.8, str(filled["due_confidence"]))
    check("标出这条被规则补过", filled["extractor"], "llm+rule")
    check_true("candidates 里能看出这个时间是谁给的",
               any(c.get("model") == "rule-engine" and c.get("due_at") == want_due
                   for c in filled["candidates"]), str(filled["candidates"]))
    check_true("原候选没被丢掉",
               any(c.get("model") == "deepseek-flash" for c in filled["candidates"]))
    check("模型自己的字段不动（标题）", filled["title"], "提交军训心得")
    check("tokens 保留", filled["tokens"], 12)
    check("提示词版本不变（模型那条路的版本）", filled["prompt_ver"], PROMPT_VER)

    already = dict(llm_no_due, due_at=1234567890000, due_confidence=0.9)
    check("模型已经给了时间 → 一个字都不改",
          fill_due_from_rule(already, with_time, source_ts=ANCHOR), already)

    check("规则也没时间 → 原样", fill_due_from_rule(dict(llm_no_due), None, source_ts=ANCHOR),
          llm_no_due)
    check("模型没结果 → 原样返回 None", fill_due_from_rule(None, with_time, source_ts=ANCHOR), None)

    # 规则算出的是"过去"（周五的消息里说"本周三"）→ 不当真（与模型的判据一致）
    friday = int(datetime(2026, 9, 18, 10, 0, tzinfo=TZ).timestamp() * 1000)
    past_rule = rule_extract("本周三前交材料", friday)
    check_true("规则确实把它算成了过去的时间", past_rule and past_rule["due_at"] < friday)
    kept = fill_due_from_rule(dict(llm_no_due), past_rule, source_ts=friday)
    check("过去的时间不补进 due_at", kept["due_at"], None)
    check("但模型自己写的 due_text 还在", kept.get("due_text"), None)

    # ------------------------------------------------------------------
    print("\n--- 6. finalize：非通知不建条、离谱时间降级、reason 不炸 ---")
    raw = {"ts": ANCHOR, "message_id": "m1", "content": "大家下周三前把军训心得交到班长那里"}
    check("模型判非通知 → None",
          finalize(LLMNotification(is_notification=False, reason="闲聊", evidence="收到"), raw, "m"),
          None)
    check("非通知且没给 reason 也不炸（旧模型可能不返回这个字段）",
          finalize(LLMNotification(is_notification=False), raw, "m"), None)
    check("是通知但 evidence 为空 → 不建条（防幻觉的硬约束）",
          finalize(LLMNotification(is_notification=True, title="x"), raw, "m"), None)
    ok = finalize(
        LLMNotification(is_notification=True, title="提交军训心得", due_at="2026-09-23T23:59:00+08:00",
                        due_text="下周三前", due_confidence=0.85, evidence="大家下周三前把军训心得交到班长那里"),
        raw, "m",
    )
    check_true("正常的一条能建", ok is not None)
    check("due_at 解析成毫秒", ok["due_at"], want_due)
    bad = finalize(
        LLMNotification(is_notification=True, title="x", due_at="2026-08-01T00:00:00+08:00",
                        due_text="8月1日", due_confidence=0.9, evidence="y"),
        raw, "m",
    )
    check("比消息还早一个月的时间被丢掉（不许当真）", bad["due_at"], None)
    check("把握归零", bad["due_confidence"], 0.0)
    check("due_text 留着给人看", bad["due_text"], "8月1日")

    print()
    if fails:
        print(f"❌ {len(fails)}/{total} 条失败：")
        for name in fails:
            print(f"   - {name}")
        return 1
    print(f"✅ {total} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
