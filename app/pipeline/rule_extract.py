# ⚠️ 与 xcollector-client/app/pipeline/rule_extract.py 是**同一份东西**（除了这段说明）。
#
# 为什么必须一致：bot 处理 OneBot 的实时消息，client 处理聊天记录库里的历史消息，
# 两边都要回答"这条像不像通知"和"截止时间是什么"。判据一旦漂移，同一条通知从两条
# 链路进来会得到两个结果（一条建条、一条当闲聊丢掉），而用户没法知道该信哪个。
#
# 改这里之前先想清楚：**另一边的同名文件也要改**。

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

# 通知类关键词 —— 同时也是"这条像不像通知"的**门槛**（见 rule_extract 的说明）：
# 只有时间词、没有这些词的消息不再建条。
#
# ⚠️ v3 把「本周 / 下周 / 之前 / 时间 / 地点」这些**时间与结构词**从词表里拿掉了：
# 它们是"什么时候/在哪儿"，不是"要做什么"。留着它们等于把门槛又拆了 ——
# 「我下周三可能去不了」正是靠"下周"混过门槛、被做成任务的。
NOTICE_KEYWORDS = re.compile(
    r"通知|公告|安排|务必|请|需要|注意|截止|报名|统计|接龙|填表|填写|提交|上交|"
    r"签到|作业|会议|活动|考试|测试|比赛|缴费|领取|参加|全体|集合|"
    r"要求|规定|提醒|重要|下学期|完成|准备|材料|清单|公示|名单"
    # v3 追加：真实语料里高频、而老词表漏掉的"要做的事"（漏掉它们的后果是
    # 「明天上午9点开会」这种最典型的通知反而不建条）
    r"|开会|班会|例会|大会|晚会|运动会|宣讲|答疑|讲座|培训|面试|答辩|体检|接种|军训|"
    r"服装|尺码|报到|登记|注册|激活|预约|报告|论文|作品|照片|电子版|扫描件|表格|"
    r"办理|申领|缴纳|选课|退课|退选|申请|核对|确认|上传|下载|观看|值班|打扫|清点|"
    r"归还|发放|签收|签字|签署|补交|退还|宿舍|寝室|校园卡|一卡通|银行卡|回执|问卷"
)

# 强调词，提升置信度
EMPHASIS = re.compile(r"务必|请|要求|全体|注意|重要|截止|必须")

# **群务**：改群名片/群昵称、拉人进群、查收名单、看群文件…… 这些是"群怎么用"，
# 不是"有人要去做的事"，永远不该出现在任务板上。命中就**一票否决**（不管模型怎么说）。
#
# 为什么必须一票否决，而不是"算一个信号"：这类消息几乎**必定**带着"请/要求/全体"
# 这些词（「请大家按照要求修改群名片」），光靠通知词的门槛拦不住；
# 而它们在真实语料里占了大头（2026-09-19 用户那份数据：317 条通知里 17 条是同一句
# 欢迎语 / 改名片提醒，还有"抓紧入群""查收班团干部名单"）。
GROUP_CHORE = re.compile(
    r"群昵称|群名片|修改名片|名片格式|改名卡|"
    r"欢迎入群|抓紧入群|进群|退群|加群|入会|查收.{0,10}名单|群成员列表|群文件|群相册"
)

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


def is_group_chore(text: str) -> str | None:
    """是"群务"吗？是就返回命中的那个词（用来写日志），不是返回 None。

    群务（改群名片、进群、查收名单、看群文件）**永远不是任务**，一票否决：
    它们带着"请/要求/全体"这些词，靠通知词门槛拦不住。
    """
    m = GROUP_CHORE.search(text or "")
    return m.group(0) if m else None


def rule_extract(content: str, ts_ms: int, at_all: bool = False) -> dict | None:
    """返回一个 notification dict（不含 raw_message_id 等由调用方补齐的字段），或 None。

    ## v3 起的门槛：**只有时间词不算通知**

    以前的条件是 `有通知关键词 or 解析出时间`，于是任何提到日期的消息都会变成一条任务 ——
    真实语料里这类误报长这样：「@张三 明天」「我下周三可能去不了」「今天是训练第二天，
    大家继续加油」。用户的原话是"很多不是通知的内容也被做成任务"。

    现在必须有**通知特征**（通知类关键词 / 强调词 / @全体）才建条：只有时间的那些交给
    模型判断（`both` 模式下模型说了算），模型也不认为是通知就不建。

    ## v4：**群务一票否决**

    2026-09-19 看用户线上那 317 条通知：被判"是通知"的一大类是群务
    （「请大家按照要求修改群名片」×17、「还没进群的抓紧了」「查收班团干部名单」
    「看群公告」）。它们命中"请/要求/全体"，所以必须单独一票否决。

    代价说清楚：`EXTRACTOR=rule` 时，一条**既没有通知词、又没有强调词**的真通知
    （"下周三体检"里的"体检"已经补进词表，但总会有漏的）会被漏掉。所以 rule 模式
    只适合"先跑通链路"，要判得准还是得用 `both`。
    """
    text = (content or "").strip()
    if is_noise(text):
        return None
    chore = is_group_chore(text)
    if chore:
        # 不静默：调用方会把 None 记成"非通知"，这里留下判据
        return None

    has_keyword = bool(NOTICE_KEYWORDS.search(text))
    has_emphasis = bool(EMPHASIS.search(text))
    guess = parse_due(text, ts_ms)

    # 通知特征：三者有其一才算"像通知"
    if not (has_keyword or has_emphasis or at_all):
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
    if has_keyword:
        confidence += 0.1
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
