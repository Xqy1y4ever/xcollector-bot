"""Xcollector bot —— 独占 OneBot 连接，把 QQ 消息规范化后推给 backend。

职责边界（很重要）：
  这一层只做「协议翻译 + 机器人交互」。
  - OneBot 协议的所有细节（消息段、合并转发、群信息查询）都锁在这个仓库里，
    backend 永远不需要知道 OneBot 长什么样；
  - bot 自己**没有数据库**：记住的只有「谁在等哪条 /add 的确认」和
    「每个用户上一次 /list 的编号」，都是进程内、可丢的状态。
    进程重启后这些状态消失是可接受的（用户重发一次即可），
    换来的是 bot 可以随便重启、随便多开一份做灰度。
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
