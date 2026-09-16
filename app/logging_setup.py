"""日志初始化。

抄 backend 的写法：抽出来共用，是因为 `python -m app.tools.send_test`
这类入口脚本如果没配置 logging，INFO 级别会走 Python 的 lastResort handler
—— 只把 WARNING 以上打到 stderr，结果就是"明明加了日志却什么都看不到"。
"""

from __future__ import annotations

import logging

from .config import get_settings

FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
DATEFMT = "%H:%M:%S"


def setup_logging(level: str | None = None) -> None:
    settings = get_settings()
    resolved = (level or settings.log_level or "INFO").upper()

    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format=FORMAT,
        datefmt=DATEFMT,
        force=True,
    )
    # 这两个库的 INFO 噪音很大（每条 HTTP 请求一行），压到 WARNING。
    # 出问题时把 LOG_LEVEL 调到 DEBUG 也还是看不到它们的明细 —— 需要时再单独放开。
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
