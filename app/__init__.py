"""Xcollector bot —— 消息处理层（连 OneBot、筛选、抽取、digest、指令）。

职责边界（很重要）：
  这一层做所有"业务判断"：连 OneBot、群/发送者白名单、抽取（规则 + LLM）、
  附件下载上传、digest 组装与发送、缺口检测、统计计数。后端只剩纯 CRUD。

  但 bot **不持有任何需要跨重启存活的状态**：
  - 待确认的 /add、/list 的编号映射 → 后端 `/api/state/*`（带 TTL）
  - digest「今天发过没有」      → 后端 `/api/digest-log`
  - 没处理完的原文              → 后端 `raw_message.state = pending`（崩溃恢复队列）
  留在内存里的只有连接对象、群名缓存，以及"连原文都没写进后端"的待重试队列
  （那一类会在日志里打 ERROR，并在 /api/status 上暴露计数）。
  这样 bot 可以随便重启、随便升级，用户不会因此收到重复的 digest 或莫名其妙的"未找到"。
"""

from __future__ import annotations

from pathlib import Path

__version__ = "0.1.0"

_BASE_DIR = Path(__file__).resolve().parent.parent

# 尽早把 .env 灌进进程环境：
# 让 `python -m app.tools.send_test` 这类入口脚本不用显式加载就能读到配置，
# 也让将来任何直接读 os.environ 的第三方库能看见同样的值。
try:
    from dotenv import load_dotenv

    load_dotenv(_BASE_DIR / ".env", override=False)
except Exception:  # dotenv 缺失不应阻止服务启动
    pass
