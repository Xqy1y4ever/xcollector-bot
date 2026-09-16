"""指令解析与排版的离线测试。

    python -m tests.check_commands

纯函数测试：不连 NapCat、不连 backend、不启服务，任何机器上都能跑。
之所以把解析和排版都做成纯函数，就是为了这一条 ——
"指令到底怎么解析"是这个仓库最容易改坏、又最难手工验证的地方。
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

# Windows 控制台默认是 GBK，直接 print 中文/emoji 会 UnicodeEncodeError。
# 测试脚本必须在打印之前把 stdout 切到 UTF-8，否则测试会"因为输出而失败"。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

from app.commands import (
    PendingAdd,
    format_add_receipt,
    format_confirm_prompt,
    format_due,
    format_due_with_source,
    format_list_reply,
    is_no,
    is_yes,
    one_line,
    pad_to,
    parse_command,
    parse_index,
    parse_list_n,
    pending_expired,
)
from app.normalize import message_kind
from app.onebot.segments import parse_message
from app.utils import truncate

TZ = timezone(timedelta(hours=8))
ANCHOR = int(datetime(2026, 9, 16, 15, 0, tzinfo=TZ).timestamp() * 1000)

failures: list[str] = []


def check(name: str, got, want) -> None:
    if got == want:
        print(f"ok    {name}")
    else:
        failures.append(name)
        print(f"FAIL  {name}\n      期望 {want!r}\n      实际 {got!r}")


def check_true(name: str, cond: bool, detail: str = "") -> None:
    check(name + (f" ({detail})" if detail else ""), bool(cond), True)


# ---------------------------------------------------------------------------
# parse_command
# ---------------------------------------------------------------------------


def test_parse_command() -> None:
    cmd = parse_command("/add 明天下午3点 交实验报告")
    check("基本指令", (cmd.name, cmd.arg), ("add", "明天下午3点 交实验报告"))

    # 大小写：/ADD 等价 /add，但参数不能被改写
    cmd = parse_command("/ADD 交实验报告")
    check("大写指令名", (cmd.name, cmd.arg), ("add", "交实验报告"))

    # 全角斜杠：中文输入法默认就是这个，不兼容的话用户会觉得机器人死了
    cmd = parse_command("／add 交实验报告")
    check("全角斜杠", (cmd.name, cmd.arg), ("add", "交实验报告"))

    cmd = parse_command("　／LIST　5　")
    check("全角空格+全角斜杠", (cmd.name, cmd.arg), ("list", "5"))

    # 前后空白
    cmd = parse_command("   /help   ")
    check("前后空白", (cmd.name, cmd.arg), ("help", ""))

    # 无参数
    cmd = parse_command("/add")
    check("无参数 add", (cmd.name, cmd.arg), ("add", ""))

    cmd = parse_command("/done")
    check("无参数 done", (cmd.name, cmd.arg), ("done", ""))

    # 未知指令照样解析出来，由路由决定怎么回
    cmd = parse_command("/foobar 1")
    check("未知指令", (cmd.name, cmd.arg), ("foobar", "1"))

    # 参数里的空格必须保留（"交实验报告 a b" 的信息量不同）
    cmd = parse_command("/add  A   B ")
    check("参数内部空格保留", (cmd.name, cmd.arg), ("add", "A   B"))

    # 不是指令的一律 None
    for text in ["y", "是的", "", "   ", "hello", "/", "／"]:
        check(f"非指令 {text!r}", parse_command(text), None)

    # "//" 会被解析成"名为 / 的未知指令"，由路由回一句"未知指令"。
    # 这不是我们想要支持的写法，但也不该让整条消息静默消失。
    cmd = parse_command("//")
    check("双斜杠 → 未知指令", (cmd.name, cmd.arg), ("/", ""))


# ---------------------------------------------------------------------------
# y/n 与编号
# ---------------------------------------------------------------------------


def test_confirm_words() -> None:
    for word in ["y", "Y", "yes", "Yes", "是", "确认", " 好 "]:
        check_true(f"yes: {word}", is_yes(word))
    for word in ["n", "N", "no", "否", "取消", "不"]:
        check_true(f"no: {word}", is_no(word))

    check("y 不是 no", is_no("y"), False)
    check("n 不是 yes", is_yes("n"), False)
    check("随便一句话不是 yes", is_yes("好的吧"), False)


def test_indices() -> None:
    check("编号 3", parse_index("3"), 3)
    check("编号 #3", parse_index("#3"), 3)
    check("编号 带空格", parse_index(" 12 "), 12)
    check("编号 0 非法", parse_index("0"), None)
    check("编号 字母非法", parse_index("abc"), None)
    check("编号 空", parse_index(""), None)
    check("编号 3.5 非法", parse_index("3.5"), None)

    check("list n 缺省", parse_list_n(""), 10)
    check("list n=5", parse_list_n("5"), 5)
    check("list n 越界收敛", parse_list_n("999"), 50)
    check("list n=0 收敛到 1", parse_list_n("0"), 1)
    check("list n 非法退回默认", parse_list_n("abc"), 10)


def test_pending_expiry() -> None:
    pending = PendingAdd(text="x", created_at=1000.0, preview={})
    check("未过期", pending_expired(pending, 600, now=1500.0), False)
    check("刚好过期", pending_expired(pending, 600, now=1600.0), False)
    check("已过期", pending_expired(pending, 600, now=1601.0), True)


# ---------------------------------------------------------------------------
# 时间与排版
# ---------------------------------------------------------------------------


def test_format_due() -> None:
    # due_at 为空 → 退回 due_text 原文，绝不显示空白
    check("空时间用 due_text", format_due(None, "尽快", 0.0), "尽快")
    check("都没有 → 待确认", format_due(None, None, 0.0), "待确认")
    check("空字符串 → 待确认", format_due(None, "  ", 0.0), "待确认")

    due = int(datetime(2026, 9, 17, 15, 0, tzinfo=TZ).timestamp() * 1000)
    check("同年只显示月日", format_due(due, None, 0.0, now=ANCHOR), "09-17 15:00")
    # 置信度 0.6~0.9 加 ~
    check("低置信度加 ~", format_due(due, None, 0.7, now=ANCHOR), "~09-17 15:00")
    check("置信度 0.9 也加 ~", format_due(due, None, 0.9, now=ANCHOR), "~09-17 15:00")
    check("高置信度不加", format_due(due, None, 0.95, now=ANCHOR), "09-17 15:00")
    check("置信度 0.5 不加", format_due(due, None, 0.5, now=ANCHOR), "09-17 15:00")

    # 跨年必须带年份，否则 "01-05" 会让人误判成下个月
    other_year = int(datetime(2027, 1, 5, 23, 59, tzinfo=TZ).timestamp() * 1000)
    check("跨年带年份", format_due(other_year, None, 0.0, now=ANCHOR), "2027-01-05 23:59")

    check(
        "回执带原文",
        format_due_with_source(due, "明天下午3点", 0.9),
        "~09-17 15:00（明天下午3点）",
    )


def test_format_receipt() -> None:
    task = {
        "id": "1757692800000abcd",
        "title": "交实验报告",
        "due_at": int(datetime(2026, 9, 17, 15, 0, tzinfo=TZ).timestamp() * 1000),
        "due_text": "明天下午3点",
        "due_confidence": 0.9,
        "location": "教三201",
    }
    text = format_add_receipt(task)
    check_true("回执含标题", "交实验报告" in text, text)
    check_true("回执含编号后 6 位", "#00abcd" in text, text)
    check_true("回执含地点", "教三201" in text, text)

    no_loc = dict(task, location=None)
    check_true("无地点写「未提到」", "未提到" in format_add_receipt(no_loc))

    no_due = dict(task, due_at=None, due_text=None)
    check_true("无截止写「待确认」", "待确认" in format_add_receipt(no_due))


def test_confirm_prompt() -> None:
    text = format_confirm_prompt({"title": "交实验报告", "due_at": None, "due_text": None})
    check_true("含「我理解为」", "我理解为：交实验报告" in text, text)
    check_true("截止写未识别", "截止：未识别" in text, text)
    check_true("问 y/n", "回复 y 确认，n 取消" in text, text)


def test_format_list() -> None:
    check("空列表", format_list_reply([], total=0), "📋 目前没有待办。")

    items = [
        {
            "id": "a",
            "title": "交实验报告",
            "due_at": int(datetime(2026, 9, 17, 15, 0, tzinfo=TZ).timestamp() * 1000),
            "due_text": None,
            "due_confidence": 0.0,
            "location": "教三201",
        },
        {
            "id": "b",
            "title": "提交军训心得",
            "due_at": None,
            "due_text": "下周三前",
            "due_confidence": 0.0,
            "location": None,
        },
        {
            "id": "c",
            "title": "安全教育平台学习",
            "due_at": None,
            "due_text": None,
            "due_confidence": 0.0,
            "location": None,
        },
    ]
    text = format_list_reply(items, total=8, shown=3)
    print("--- /list 输出样例 ---")
    print(text)
    print("----------------------")
    check_true("表头", text.startswith("📋 待办 8 条（显示前 3）"), text.splitlines()[0])
    check_true("编号 1/2/3", all(f"{i}." in text for i in (1, 2, 3)), text)
    check_true("编号缺失时退回 due_text", "下周三前" in text)
    check_true("都没有时写待确认", "待确认" in text)
    check_true("含 /done 提示", "/done <编号>" in text)
    check_true("行数 = 表头 + 3 行 + 提示", len(text.splitlines()) == 5, str(len(text.splitlines())))

    # 超长必须截断（QQ 有长度上限）
    many = [dict(items[0], title=f"任务{i}") for i in range(200)]
    long_text = format_list_reply(many, total=200)
    check_true("超长截断", len(long_text) <= 1500 + len("…（已截断）"), str(len(long_text)))
    check_true("截断后缀", long_text.endswith("…（已截断）"))


def test_width_helpers() -> None:
    check("中文算 2 列", pad_to("交实验报告", 16), "交实验报告" + " " * 6)
    check("英文算 1 列", pad_to("abcd", 6), "abcd  ")
    # 已经超宽时不再补空格，但**至少留一个**当列分隔符，
    # 否则超长标题会和后面的时间/地点黏在一起
    check("超宽只留分隔空格", pad_to("交实验报告交实验报告", 4), "交实验报告交实验报告 ")
    check("one_line 压平换行", one_line("a\n b\tc  d"), "a b c d")
    check("truncate", truncate("abcdef", 3, "…"), "abc…")
    check("truncate 不超则不截", truncate("abc", 3, "…"), "abc")


# ---------------------------------------------------------------------------
# 事件分类与消息段（归一化的上游）
# ---------------------------------------------------------------------------


def test_message_kind() -> None:
    base = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 999,
        "user_id": 10001,
    }
    check("群消息", message_kind(base), "group")
    check("私聊", message_kind(dict(base, message_type="private")), "private")
    # 机器人自己发的必须挡掉，否则回复会被 NapCat 再推回来形成自环
    check("自己发的群消息", message_kind(dict(base, user_id=999)), None)
    check("自己发的私聊", message_kind(dict(base, user_id=999, message_type="private")), None)
    check("心跳", message_kind({"post_type": "meta_event"}), None)
    check("缺 message_type", message_kind({"post_type": "message", "self_id": 9, "user_id": 1}), None)


def test_segments_passthrough() -> None:
    """segments.py 是从 backend 原样复制的，这里只钉住它最关键的几个行为。"""
    parsed = parse_message(
        [
            {"type": "at", "data": {"qq": "all"}},
            {"type": "text", "data": {"text": " 大家好"}},
            {"type": "image", "data": {"url": "https://e/x.jpg"}},
            {"type": "reply", "data": {"id": "90001"}},
            {"type": "forward", "data": {"id": "fwd-1"}},
        ]
    )
    check("at_all", parsed.at_all, True)
    check("提及列表为空", parsed.mentions, [])
    check("图片占位", "[图片]" in parsed.text, True)
    check("回复 id", parsed.reply_id, "90001")
    check("转发 id", parsed.forwards, ["fwd-1"])
    check("附件数", len(parsed.attachments), 1)
    check("附件 url", parsed.attachments[0].url, "https://e/x.jpg")

    cq = parse_message("[CQ:at,qq=all] 通知正文")
    check("CQ 码字符串", (cq.at_all, cq.text), (True, "@全体成员 通知正文"))


def main() -> int:
    for fn in (
        test_parse_command,
        test_confirm_words,
        test_indices,
        test_pending_expiry,
        test_format_due,
        test_format_receipt,
        test_confirm_prompt,
        test_format_list,
        test_width_helpers,
        test_message_kind,
        test_segments_passthrough,
    ):
        print(f"--- {fn.__name__} ---")
        fn()

    print()
    if failures:
        print(f"共 {len(failures)} 项失败：{failures}")
        return 1
    print("全部通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
