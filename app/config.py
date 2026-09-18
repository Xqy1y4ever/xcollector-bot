"""配置层。

设计原则和 backend 一致：所有配置都有可用默认值，`.env` 缺失时服务仍能启动，
这样"最小可行性验证"（不开 NapCat、不开后端）不会被配置问题卡住。

拆分之后，**所有业务开关都搬到了 bot 这边**：后端只剩存储，不认识
群白名单、LLM、digest 这些词。所以这个文件现在同时管两件事：
  1. bot 自己的运行参数（OneBot、HTTP、指令）；
  2. 原先在后端 `config.py` 里的抽取 / digest / 白名单配置（原样搬过来，
     默认值保持一致，避免"搬家"悄悄改变了系统行为）。
"""

from __future__ import annotations

from datetime import timedelta, timezone, tzinfo
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


def _fallback_tz() -> tzinfo:
    """Windows 上如果没有 tzdata 包，ZoneInfo 会失败，退化为固定 UTC+8。"""
    return timezone(timedelta(hours=8))


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

    # ---------------- 本服务（前端直连 /api/status，后端调 /api/send/*）----------------
    # 注意：bot_listen_port 和 onebot_listen_port 是两个完全不同的端口，
    # 前者是「外部 → bot」的 HTTP 入口，后者只在 server 模式下给 NapCat 反向 WS 用。
    bot_listen_host: str = "127.0.0.1"
    bot_listen_port: int = 8082
    bot_api_token: str = ""
    # 网页令牌：前端登录页用。对本服务只能调 GET /api/status 与
    # GET /api/digest/preview —— 发消息类的接口一律 403（见 app/auth.py）。
    # 留空 = 退回单令牌模式（管理令牌同时当网页令牌用）。
    # 必须与 backend 的 WEB_API_TOKEN 是**同一个值**（有自检脚本守这条）。
    web_api_token: str = ""

    # ---------------- 后端（纯数据层）----------------
    backend_base_url: str = "http://127.0.0.1:8000"
    # 调后端时带的令牌。必须是**写入令牌**（契约里两边都叫 API_TOKEN），
    # 网页令牌调写接口会拿到 403。见 app/auth.py。
    # 所以这里不再有 INGEST_API_TOKEN 这个第二名字。
    api_token: str = ""
    backend_timeout: float = 15.0
    # 「重试 3 次」= 首次失败后再试 3 次，退避 1s/2s/4s
    backend_max_retries: int = 3
    # 写后端彻底失败后，先把这条写入放进内存待重试队列再报错（见 backend_client）。
    # 队列上限：满了丢最旧的，只保留最近的失败写入。
    pending_write_max: int = 200
    # 待重试写入的扫描周期（秒）与最大尝试次数
    pending_retry_interval: float = 60.0
    pending_retry_max_attempts: int = 5

    # ---------------- 群 / 发送者白名单 ----------------
    # **两个都是白名单，留空 = 谁都不放行。** bot 只处理同时满足
    # 「群在 GROUP_WHITELIST」**且**「发送者在 SENDER_WHITELIST」的消息，
    # 其余的在**写库之前**就被丢掉（群里不进后端、库里不留痕）。
    #
    # 为什么默认关死：这套东西只该处理你明确列出来的官方通知群和发布者。
    # 空名单放开等于"随便哪个群、群里随便谁说话都会入库"，那不是它的用途。
    group_whitelist: str = ""
    sender_whitelist: str = ""
    # strict=只有名单里的发送者算（默认）；
    # off=**不限制发送者**（这个群里谁发的都算）—— 显式选择，不是默认行为
    sender_whitelist_mode: Literal["strict", "off"] = "strict"

    # ---------------- 抽取 ----------------
    # rule=只跑规则（不调用模型）；llm=只跑模型；both=模型 + 规则交叉判断
    extractor: Literal["llm", "rule", "both"] = "llm"

    # 模型拆成**提供商 + 模型名**两段配：
    #   提供商决定用哪种协议、默认打到哪个地址（见 app/llm/providers.py）
    #   模型名原样发给对方，不解析
    # 两段分开配之后，模型名里带斜杠也不会歧义：
    #   LLM_PRIMARY_PROVIDER=openrouter
    #   LLM_PRIMARY_MODEL=anthropic/claude-sonnet-4
    llm_primary_provider: str = "deepseek"
    llm_primary_model: str = "deepseek-chat"

    # 想用没内置的端点（自建、中转站、公司内网关）时填这三个，
    # 提供商名字随便起，只要有 API_BASE 就行：
    #   LLM_PRIMARY_PROVIDER=myproxy
    #   LLM_PRIMARY_API_BASE=https://llm.corp.example.com/v1
    #   LLM_PRIMARY_API_KEY=xxx
    llm_primary_api_base: str = ""
    llm_primary_api_key: str = ""
    # 自定义端点走哪种协议：openai（默认）/ google
    llm_primary_api_kind: str = ""

    # 次模型：留空 = 关闭交叉验证。四个字段与主模型一一对应。
    llm_secondary_provider: str = ""
    llm_secondary_model: str = ""
    llm_secondary_api_base: str = ""
    llm_secondary_api_key: str = ""
    llm_secondary_api_kind: str = ""

    llm_temperature: float = 0.0
    llm_max_retries: int = 2
    llm_timeout: int = 60
    vlm_enabled: bool = False
    vlm_max_images: int = 3

    # ---------------- digest ----------------
    digest_enabled: bool = True
    digest_time: str = "21:30"
    digest_tz: str = "Asia/Shanghai"
    # 收 digest 的 QQ 号（私聊）
    digest_target_qq: str = ""
    # 自动 digest 每天最多尝试几次（失败重试用；成功与否由后端的 digest_log 决定）
    digest_max_auto_attempts: int = 3

    # ---------------- 附件 ----------------
    media_download_enabled: bool = True
    media_max_bytes: int = 5 * 1024 * 1024

    # ---------------- 阈值 ----------------
    gap_alert_hours: float = 2.0
    low_confidence_threshold: float = 0.6

    # ---------------- 指令 ----------------
    command_prefix: str = "/"
    command_whitelist: str = ""
    pending_ttl_seconds: int = 600
    # /list 的「编号 → 通知 id」映射只在短期内有效：编号是给人看的，
    # 放太久之后用户对着一份过期列表敲 /done 3，操作到的会是别的条目。
    list_state_ttl_seconds: int = 1800

    # ---------------- 崩溃恢复 ----------------
    # 启动时 / OneBot 重连成功后，多久检查一次"要不要把 pending 消息捡回来"
    recovery_check_interval: float = 15.0

    # ---------------- 日志 ----------------
    log_level: str = "INFO"
    log_preview_chars: int = 60

    # ---------------- 派生属性 ----------------

    @property
    def command_whitelist_map(self) -> dict[str, str]:
        return parse_id_name_pairs(self.command_whitelist)

    def in_command_whitelist(self, user_id: str | int) -> bool:
        """白名单为空时**不允许**任何人发指令。

        和群白名单的语义相反（那边空=不限制）。理由：
        群白名单空着只是多收点消息，指令白名单空着放开就等于把机器人交出去了。
        默认安全 > 默认方便。
        """
        return str(user_id) in self.command_whitelist_map

    @property
    def group_whitelist_map(self) -> dict[str, str]:
        return parse_id_name_pairs(self.group_whitelist)

    @property
    def sender_whitelist_map(self) -> dict[str, str]:
        return parse_id_name_pairs(self.sender_whitelist)

    @property
    def group_display_names(self) -> dict[str, str | None]:
        """群白名单的显示名；**没写名字时返回 None 而不是把群号当名字**。

        `GROUP_WHITELIST=123456789` 这种写法很常见，如果直接把 id 当名字，
        状态页就会显示「群名：123456789」，看起来像解析错了。
        返回 None 之后前端会退化成显示群号，语义清楚得多。
        真实群名由 bot 通过 get_group_info 随消息带过来。
        """
        return {k: (None if v == k else v) for k, v in self.group_whitelist_map.items()}

    @property
    def group_ids(self) -> set[str]:
        return set(self.group_whitelist_map.keys())

    def in_group_whitelist(self, group_id: str | int) -> bool:
        """群白名单为空时**不处理任何群**（fail-closed）。

        留空放开等于"bot 在它能收到的每个群里都干活"，那不是它的用途 ——
        官方通知只发在固定几个群，其余的全是噪音。
        宁可一开始什么都不收（启动日志会 WARNING），也不要静默地全收。
        """
        if not self.group_whitelist_map:
            return False
        return str(group_id) in self.group_whitelist_map

    def in_sender_whitelist(self, sender_id: str | int) -> bool:
        """发送者白名单为空时**不处理任何发送者**（fail-closed）。

        群白名单只限制了"在哪个群"。一个群里几百人，谁说话都会被处理 ——
        官方通知只由固定的几个人发布，所以这里再收一道。

        `sender_whitelist_mode=off` 是**显式**的逃生口：这个群里谁发的都算。
        默认的 strict 不会因为名单为空而放开。
        """
        if self.sender_whitelist_mode == "off":
            return True
        if not self.sender_whitelist_map:
            return False
        return str(sender_id) in self.sender_whitelist_map

    @property
    def whitelist_ready(self) -> bool:
        """白名单是否配到了"能收到东西"。

        两个都空 = bot 一条消息都不会处理。启动时必须把这件事说清楚，
        否则表现出来就是"bot 连上了但什么都不干"，很难查。
        """
        if not self.group_whitelist_map:
            return False
        if self.sender_whitelist_mode == "strict" and not self.sender_whitelist_map:
            return False
        return True

    @property
    def inbound_token(self) -> str:
        """本服务 /api/* 的**写入**令牌（管理令牌）。

        契约说共享密钥都叫 API_TOKEN，所以允许 BOT_API_TOKEN 留空时退化用
        API_TOKEN —— 少配一个密钥就少一处配错的机会。
        **这个值不要给浏览器。**
        """
        return self.bot_api_token or self.api_token

    @property
    def web_scope_separated(self) -> bool:
        """网页令牌与管理令牌是否**真的**分开了。配成同一个值等于没拆。"""
        web = self.web_api_token.strip()
        write = self.inbound_token.strip()
        return bool(write and web and web != write)

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

    @property
    def digest_hhmm(self) -> tuple[int, int]:
        try:
            hh, mm = self.digest_time.strip().split(":")
            return max(0, min(23, int(hh))), max(0, min(59, int(mm)))
        except Exception:
            return 21, 30

    @property
    def tz(self) -> tzinfo:
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(self.digest_tz)
        except Exception:
            return _fallback_tz()

    @property
    def cross_check_enabled(self) -> bool:
        """次模型配了、而且和主模型不是同一个，才做交叉验证。

        "同一个"要按**提供商 + 模型名**一起比：换个提供商打同一个模型名，
        也算不同的模型（不同端点、不同权重），交叉验证照样有意义。
        """
        sec = self.llm_secondary_model.strip()
        if not sec:
            return False
        if sec != self.llm_primary_model.strip():
            return True
        return self.llm_secondary_provider.strip() != self.llm_primary_provider.strip()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
