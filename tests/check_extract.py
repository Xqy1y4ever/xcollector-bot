"""抽取层自检：提示词里的日历、判定门槛、群务一票否决、以及"模型没算出时间就用规则补"。

    .\\venv\\Scripts\\python.exe -m tests.check_extract

⚠️ 这份文件是从 `xcollector-client/tests/check_extract.py` **抄过来的**，只改了两处：
去掉那里的 `isolate_settings()`；把"真的走一遍抽取链路"的适配函数换成 `runner.parse_content`
（并且 acompletion 的打桩点换成 `app.llm` 包上的属性，因为 bot 的 extract 是延迟导入的）。
两个仓库必须用同一套判据与同一张日历 —— 同一条消息从实时链路（bot）和从聊天记录库
（client）进来，结果不能不一样。**改一边就要改另一边。**

最后一节会真的走一遍「拼提示词 → 调模型 → 合并规则」（只把网络层换成假的），
并且钉住"群务/闲聊**不管模型怎么说**都不建条，而且不花那次调用"。
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime

import app.llm as llm_pkg
from app.config import Settings, get_settings
from app.pipeline import extract as ex_mod
from app.pipeline.extract import (
    LLMNotification,
    PROMPT_VER,
    build_calendar,
    fill_due_from_rule,
    finalize,
    render_system_prompt,
)
from app.pipeline.rule_extract import rule_extract
from app.pipeline.runner import _merge_rule_disagreement as merge_rule_disagreement
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


def run_llm_path(text: str, ts_ms: int):
    """把这条消息**真的**走一遍抽取链路（`runner.parse_content`），网络层由调用方换掉。

    ⚠️ 两个仓库唯一不同的地方就是这一小段：client 那边走的是 `process.extract`。
    其余断言两边逐字一致。
    """
    from app.pipeline.runner import parse_content

    raw = {
        "content": text,
        "ts": ts_ms,
        "message_id": "m1",
        "group_id": "g1",
        "sender_id": "s1",
        "sender_name": "老师",
        "at_all": False,
    }
    settings = Settings(
        extractor="both",
        llm_primary_provider="deepseek",
        llm_primary_model="fake-model",
        deepseek_api_key="fake-key",
    )
    return asyncio.run(parse_content(raw, settings, []))


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
    check("提示词版本是 v4", PROMPT_VER, "llm-v4")
    # v4 的重点：把"有人要做事"改成"该进任务板的官方通知"，并列出真实语料里的误报类型
    for needle in ("回执与附和", "提问与追问", "已发生事情的回顾", "非官方内容",
                   "只有称呼或寒暄", "拿不准就判 false",
                   "把群务和闲聊做成任务，比漏掉一条真通知更烦人",
                   "群务/群管理", "对某一个人说的话", "口头催促", "可选参加", "纯提醒",
                   "转发、链接分享", "改群名片", "查收名单", "欢迎大家报名参加"):
        check_true(f"列了这类非通知：{needle}", needle in prompt)
    check_true("三条必要条件写清了（发给全群 / 具体的事 / 时间地点或正式口径）",
               "是**发给全群**的话" in prompt and "有一个**具体要做的事**" in prompt
               and "**能指出时间或地点**" in prompt)
    check_true("写明「写了时间地点的讲座/活动预告要判 true」（别把真安排一起砍掉）",
               "写了时间/地点/报名截止的讲座、活动、比赛预告要判 true" in prompt)
    check_true("写明「@全体成员 + 具体动作的催办算 true」",
               "对**全群**的、带具体动作的催办要判 true" in prompt)
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
        # --- v4：群务一票否决（线上 317 条里这类占 33 条）---
        ("欢迎大家入群，请大家按照要求修改群名片，格式是【年级 院系 姓名】", False),
        ("@某人 请修改群名片", False),
        ("还没进群的抓紧了", False),
        ("还没入会的同学请抓紧～ @全体成员", False),
        ("请各位同学查收班团干部名单~", False),
        # 这条是"链接分享"，规则层认不出来（它有"下载/查阅"这种动作词）→ 交给模型判；
        # 这里钉的是"规则层别误杀"，别把期望写成 False。
        ("下载《学在鼓楼》查阅教室分布：https://jw.nju.edu.cn/x", True),
        ("各位同学：请完成群昵称修改（姓名 专业)，管理员会核实清理。", False),
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

    # ------------------------------------------------------------------
    print("\n--- 7. 真的走一遍「拼提示词 → 调模型 → 合并规则」这条路（网络层换成假的）---")
    # 为什么必须有这一节：上面全是纯函数，而真正会坏的是**拼提示词 + 调模型**这一段。
    # 现实教训：`build_system_prompt` 改名成 `render_system_prompt` 之后，`_call_model`
    # 里的调用处没跟着改 —— 每次模型调用都变成 `NameError`，被上层吞成
    # "降级为规则抽取"，而**所有自检都是绿的**（没有任何测试真的调用过这条路）。
    llm_calls: list[dict] = []
    reply = {
        "is_notification": True,
        "reason": "",
        "title": "提交军训心得",
        "summary": "下周三前把军训心得交给班长。",
        "location": None,
        "due_at": None,           # ← 故意不给时间：看规则兜底接不接得住
        "due_text": "下周三前",
        "due_confidence": 0.0,
        "evidence": "大家下周三前把军训心得交到班长那里",
    }

    class _FakeCompletion:
        total_tokens = 7

        def __init__(self, payload: dict):
            # 必须**每次调用时**再序列化：写成类属性的话，第一次求值之后就被冻住了，
            # 后面改 `reply` 不会生效（第一版就踩了这个坑：第二个用例拿到的还是第一个的回答）。
            self.text = json.dumps(payload, ensure_ascii=False)

    async def _fake_acompletion(**kwargs):
        llm_calls.append(kwargs)
        return _FakeCompletion(reply)

    original = llm_pkg.acompletion
    try:
        llm_pkg.acompletion = _fake_acompletion
        result, degraded, tokens = run_llm_path("大家下周三前把军训心得交到班长那里", ANCHOR)
    finally:
        llm_pkg.acompletion = original

    check("这条路跑通了（没有降级）", degraded, False)
    check("调了 1 次模型", len(llm_calls), 1)
    check_true("拿回的是模型的结果", result is not None and result["title"] == "提交军训心得", str(result))
    check("token 数带回来了", tokens, 7)
    system = llm_calls[0]["messages"][0]["content"]
    check_true("提示词里带着按发送时间算好的日历",
               "本周：周一 09-14" in system and "下周：周一 09-21" in system)
    check_true("提示词里带着消息发送时间", "2026-09-16 15:00" in system)
    check_true("提示词里没有没替换的占位符",
               all(p not in system for p in ("{send_time}", "{weekday}", "{tz}", "{calendar}")))
    check("模型没给 due_at → 规则把它补上了", result["due_at"], want_due)
    check("并如实标出这条被规则补过", result["extractor"], "llm+rule")
    check("due_text 用规则解析出的时间短语", result["due_text"], "下周三前")
    check_true("把握不超过 0.8", 0 < float(result["due_confidence"]) <= 0.8, str(result["due_confidence"]))

    # 模型判"不是通知"、规则也没有明确时间 → 不建条（别让闲聊变成任务）
    reply.update({"is_notification": False, "reason": "回执", "title": "", "due_text": None})
    try:
        llm_pkg.acompletion = _fake_acompletion
        result2, degraded2, _tokens2 = run_llm_path("收到", ANCHOR)
    finally:
        llm_pkg.acompletion = original
    check("模型判非通知 + 规则也没有时间 → 不建条", result2, None)
    check("而且不算降级（模型正常回答了）", degraded2, False)

    # 确定性判据必须**压过模型**：模型说"是通知"也不行，而且**不发那次请求**（省钱）。
    # 线上真事：v4 提示词里明明写了"群务不算通知"，模型还是把
    # 「各位同学：请完成群昵称修改…」判成了一条通知（因为一票否决只长在 rule_extract 里，
    # llm 模式下模型说了算，它从旁边绕过去了）。
    reply.update({"is_notification": True, "title": "按时报到", "due_text": "8月22日",
                  "evidence": "请完成群昵称修改", "due_at": None})
    calls_before = len(llm_calls)
    try:
        llm_pkg.acompletion = _fake_acompletion
        vetoed, vetoed_degraded, _t = run_llm_path(
            "各位同学：请完成群昵称修改（姓名 专业)，管理员会核实清理。", ANCHOR
        )
    finally:
        llm_pkg.acompletion = original
    check("群务：模型说是通知也不算 → 不建条", vetoed, None)
    check("而且**没花**那次模型调用（确定性判据在调用之前）", len(llm_calls), calls_before)
    check("也算不上降级", vetoed_degraded, False)

    reply.update({"is_notification": True, "title": "闲聊", "evidence": "收到", "due_text": None})
    try:
        llm_pkg.acompletion = _fake_acompletion
        noise_vetoed, _d, _t = run_llm_path("收到", ANCHOR)
    finally:
        llm_pkg.acompletion = original
    check("闲聊/回执同理：不建条", noise_vetoed, None)
    check("也没发请求", len(llm_calls), calls_before)

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
