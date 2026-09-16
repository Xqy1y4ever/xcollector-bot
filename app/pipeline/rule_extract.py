"""确定性规则抽取器。

存在的意义有两个：
  1. 没有 API key / LLM 不可用时，整条链路（入库 → 抽取 → API → 前端 → digest）
     依然能跑通，这才是"最小可行性验证"。
  2. 给 LLM 的结果做 sanity check。

它不追求准确率，只追求"不漏掉有明显时间信息的官方通知"。
"""

from __future__ import annotations

import re

from .timeparse import parse_due, sentence_around

# 明显的闲聊/回执，直接判为非通知
NOISE_EXACT = {
    "收到", "好的", "好", "谢谢", "谢谢老师", "辛苦", "辛苦了", "明白", "了解",
    "ok", "OK", "Ok", "嗯", "嗯嗯", "哈哈", "哈哈哈", "在吗", "1", "+1", "顶",
    "已阅", "赞", "👍", "谢谢老板", "老师好", "早上好", "晚安",
}

# 通知类关键词
NOTICE_KEYWORDS = re.compile(
    r"通知|公告|安排|务必|请|需要|注意|截止|报名|统计|接龙|填表|填写|提交|上交|"
    r"签到|作业|会议|活动|考试|测试|比赛|缴费|领取|参加|全体|集合|时间|地点|"
    r"要求|规定|提醒|重要|下学期|本周|下周|之前|完成|准备|材料|清单|公示|名单"
)

# 强调词，提升置信度
EMPHASIS = re.compile(r"务必|请|要求|全体|注意|重要|截止|必须")

# 地点：规则抽取不可能做得好，只求"原文明确写了、且能高置信度认出"这几种形态。
# 三种优先级从高到低：
#   1. 「地点：xxx」显式标注
#   2. 「在/于/到 + 以场所后缀结尾的词」——如 在教三201、到学工办、于体育馆
#   3. 「在/于/到 + 楼名+房间号」——如 在教三201、于主教304（中文场所名常不带后缀）
# 通不过就返回 None，交给 LLM 那条路。宁可没有地点，也不要抽出一个错的。
VENUE_SUFFIX = (
    r"(?:号楼|阶梯教室|会议室|办公室|学工办|教室|体育馆|图书馆|操场|广场|中心|校区|楼|馆|厅)"
)
PAT_LOCATION = re.compile(
    r"(?:地点|地址|教室)\s*[:：]\s*([^\s，。；,;、]{2,20})"
    # 前缀用**贪婪**匹配 + 排除数字，否则：
    #   懒匹配会跨过整个句子（「于9月20日前到图书馆」被当成地点），
    #   而数字会让你停在房间号中间。
    # {0,12} 允许前缀为空，否则「学工办」这种整体就是场所名的词匹配不上。
    r"|(?:在|于|到|去)\s*([^\s，。；,;、0-9]{0,12}" + VENUE_SUFFIX + r")"
    r"|(?:在|于|到|去)\s*([\u4e00-\u9fff]{1,4}\d{3,4})"
)

MAX_TITLE = 40
MAX_SUMMARY = 160


def rule_location(text: str) -> str | None:
    """从原文里认出一个明确写出的地点，认不出返回 None。"""
    if not text:
        return None
    m = PAT_LOCATION.search(text)
    if not m:
        return None
    for group in m.groups():
        if group:
            value = group.strip()[:60]
            # 「在…上」「到…中」这类多半是虚指，不是地点
            if value and not value.endswith(("上", "中", "下", "时")):
                return value
    return None


def _strip_leading_marks(text: str) -> str:
    return re.sub(r"^\s*(?:\[@全体成员\]|@全体成员|@所有人|【[^】]{0,12}】|\[[^\]]{0,12}\])\s*", "", text).strip()


def is_noise(text: str) -> bool:
    t = text.strip()
    if not t:
        return True
    if t in NOISE_EXACT:
        return True
    if len(t) <= 4:
        return True
    # 全是占位符（纯图片/表情）
    if re.fullmatch(r"(?:\[[^\]]{1,6}\])+", t):
        return True
    return False


def rule_extract(content: str, ts_ms: int, at_all: bool = False) -> dict | None:
    """返回一个 notification dict（不含 raw_message_id 等由调用方补齐的字段），或 None。"""
    text = (content or "").strip()
    if is_noise(text):
        return None

    has_keyword = bool(NOTICE_KEYWORDS.search(text))
    has_emphasis = bool(EMPHASIS.search(text))
    guess = parse_due(text, ts_ms)

    # 既没有通知关键词，也没有时间信息 → 不像任务，交给 LLM 那条路去处理
    if not has_keyword and guess is None:
        return None

    body = _strip_leading_marks(text)
    first_line = body.split("\n", 1)[0].strip()
    title = first_line[:MAX_TITLE] or body[:MAX_TITLE] or "未命名通知"

    if guess is not None and guess.sentence:
        evidence = guess.sentence
    else:
        evidence = sentence_around(body, 0, min(len(body), 20))

    if not evidence.strip():
        # evidence 必须非空，这是硬约束
        evidence = body[:120]

    confidence = 0.45
    if guess is not None:
        confidence += 0.3
    if has_emphasis:
        confidence += 0.1
    if at_all:
        confidence += 0.1
    confidence = round(min(confidence, 0.95), 2)

    return {
        "title": title,
        "summary": body[:MAX_SUMMARY] if body else None,
        "location": rule_location(body),
        "due_at": guess.due_at if guess else None,
        "due_text": guess.due_text if (guess and guess.due_text) else None,
        "due_confidence": guess.confidence if guess else 0.0,
        "confidence": confidence,
        "evidence": evidence.strip(),
        "extractor": "rule",
        "model": "rule-engine",
        "prompt_ver": "rule-v1",
        "conflict": False,
        "candidates": [],
    }
