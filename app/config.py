"""配置层。

设计原则和 backend 一致：所有配置都有可用默认值，`.env` 缺失时服务仍能启动，
这样"最小可行性验证"（不开 NapCat、不开后端）不会被配置问题卡住。
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


def parse_id_name_pairs(raw: str) -> dict[str, str]:
    """解析 `id:备注,id:备注` 形式的配置，返回 {id: 备注}。

    - 备注可省略（只写 id），此时备注取 id 本身，日志里就不会出现空括号；
    - 冒号兼容中文全角「：」，逗号兼容「，」—— 这两样都是在手机上手打配置时的高频错误。
    """
    out: dict[str, str] = {}
    for chunk in (raw or "").replace("，", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        id_part, name = chunk, ""
        for sep in (":", "："):
            if sep in chunk:
                id_part, name = chunk.split(sep, 1)
                break
        id_part = id_part.strip()
        if not id_part:
            continue
        out[id_part] = name.strip() or id_part
    return out


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---------------- OneBot ----------------
    onebot_mode: Literal["client", "server"] = "client"
    onebot_ws_url: str = "ws://127.0.0.1:3001"
    onebot_access_token: str = ""
    onebot_listen_host: str = "0.0.0.0"
    onebot_listen_port: int = 8081
    onebot_listen_path: str = "/onebot/ws"
    forward_max_depth: int = 3

    # ---------------- 本服务（后端会调）----------------
    # 注意：bot_listen_port 和 onebot_listen_port 是两个完全不同的端口，
    # 前者是「后端 → bot」的 HTTP 入口，后者只在 server 模式下给 NapCat 反向 WS 用。
    bot_listen_host: str = "127.0.0.1"
    bot_listen_port: int = 8082
    bot_api_token: str = ""

    # ---------------- 后端 ----------------
    backend_base_url: str = "http://127.0.0.1:8000"
    backend_api_token: str = ""
    backend_timeout: float = 15.0
    # 「重试 3 次」= 首次失败后再试 3 次，退避 1s/2s/4s
    backend_max_retries: int = 3

    # ---------------- 指令 ----------------
    command_prefix: str = "/"
    command_whitelist: str = ""
    pending_ttl_seconds: int = 600

    # ---------------- 日志 ----------------
    log_level: str = "INFO"
    log_preview_chars: int = 60

    # ---------------- 派生属性 ----------------

    @property
    def command_whitelist_map(self) -> dict[str, str]:
        return parse_id_name_pairs(self.command_whitelist)

    def in_command_whitelist(self, user_id: str | int) -> bool:
        """白名单为空时**不允许**任何人发指令。

        和 backend 的群白名单语义相反（那边空=不限制）。理由：
        群白名单空着只是多收点消息，指令白名单空着放开就等于把机器人交出去了。
        默认安全 > 默认方便。
        """
        return str(user_id) in self.command_whitelist_map

    @property
    def backend_base(self) -> str:
        return self.backend_base_url.rstrip("/")

    @property
    def onebot_server_target(self) -> str:
        """server 模式下对外展示的监听地址，用于 /api/status 的 target 字段。"""
        return (
            f"{self.onebot_listen_host}:{self.onebot_listen_port}"
            f"{self.onebot_listen_path}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
