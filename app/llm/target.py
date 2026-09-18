"""调用目标：**提供商 + 模型名**（外加可选的地址 / 密钥 / 协议覆盖）。

为什么把它拆成两个配置，而不是写成一个 `deepseek/deepseek-chat`：

  - 模型名**本身可能带斜杠**（`anthropic/claude-sonnet-4` 走 openrouter），
    塞进一个字符串里就分不清哪一段是提供商、哪一段是模型。
  - 想接没内置的端点时，需要能**单独**指定 base URL 和协议，
    而不是只能去猜"该设哪个环境变量"。

于是职责切成两半：提供商决定**协议和默认地址**（见 providers.py 的注册表），
模型名原样转发不做解析；要改地址/密钥/协议就在各自的字段里覆盖。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any

from .errors import LLMConfigError
from .providers import PROVIDERS, Provider

logger = logging.getLogger(__name__)

# 兼容旧写法时只提醒一次（target_from_settings 是逐条消息调用的）
_legacy_warned: set[tuple[str, str]] = set()


@dataclass(frozen=True)
class LLMTarget:
    """一次模型调用要用的全部端点信息。"""

    provider: str
    model: str
    api_base: str = ""
    api_key: str = ""
    api_kind: str = ""

    @property
    def is_configured(self) -> bool:
        return bool(self.provider.strip() and self.model.strip())

    @property
    def label(self) -> str:
        """写进库、显示给用户看的模型标识（`提供商/模型名`）。

        和旧的单字符串写法长得一样，所以已有的数据、前端展示都不受影响。
        """
        return f"{self.provider}/{self.model}" if self.provider else self.model

    def describe(self) -> str:
        """给人看的描述。**key 只说有没有，不说内容。**"""
        parts = [f"{self.provider}/{self.model}"]
        if self.api_base:
            parts.append(f"base={self.api_base}")
        if self.api_kind:
            parts.append(f"kind={self.api_kind}")
        parts.append("key=已单独配置" if self.api_key else "key=按提供商默认")
        return "  ".join(parts)


def build_provider(
    name: str,
    *,
    api_base: str = "",
    api_key: str = "",
    api_kind: str = "",
) -> Provider:
    """按名字取端点描述，并用显式配置覆盖。

    - 名字在注册表里 → 用注册表的协议/默认地址/密钥变量名
    - 名字不在注册表里 → **必须**给 api_base，然后按 OpenAI 兼容端点处理
      （要 Google 协议就把 api_kind 设成 google）
    """
    key = (name or "").strip().lower()
    if not key:
        raise LLMConfigError(
            "没有配置模型提供商。请设置 LLM_PRIMARY_PROVIDER（例如 deepseek、google、openai），"
            "或接自建端点时同时设置 LLM_PRIMARY_API_BASE。"
        )

    base = (api_base or "").strip()
    override_kind = (api_kind or "").strip().lower()

    if override_kind and override_kind not in ("openai", "google"):
        raise LLMConfigError(
            f"API_KIND={api_kind!r} 不认识，只能是 openai 或 google"
        )

    registered = PROVIDERS.get(key)
    if registered is None:
        if not base:
            known = ", ".join(sorted(PROVIDERS))
            raise LLMConfigError(
                f"未知的模型提供商 {name!r}。已内置：{known}。"
                f"如果是自建的 OpenAI 兼容服务，请再设置 API_BASE"
                f"（例如 LLM_PRIMARY_API_BASE=https://your-host/v1），"
                f"或设 {key.upper().replace('-', '_')}_API_BASE。"
            )
        return Provider(
            key,
            override_kind or "openai",  # type: ignore[arg-type]
            base,
            (),
            requires_key=False,  # 自建服务不强制要 key
            key_override=api_key.strip(),
        )

    changes: dict[str, Any] = {}
    if base:
        changes["base_url"] = base
    if override_kind:
        changes["kind"] = override_kind
    if api_key.strip():
        changes["key_override"] = api_key.strip()
    return replace(registered, **changes) if changes else registered


def target_from_settings(settings: Any, which: str = "primary") -> LLMTarget:
    """从 bot 配置里拼出一个调用目标。

    `which` 是 "primary" / "secondary" —— 两套字段同名不同前缀。
    （这里刻意把字段名写全，不用 f-string 拼 getattr：拼出来的名字
     静态检查脚本看不见，会被当成"声明了但没人读的配置"。）
    """
    if which == "secondary":
        provider = settings.llm_secondary_provider
        model = settings.llm_secondary_model
        api_base = settings.llm_secondary_api_base
        api_key = settings.llm_secondary_api_key
        api_kind = settings.llm_secondary_api_kind
    else:
        provider = settings.llm_primary_provider
        model = settings.llm_primary_model
        api_base = settings.llm_primary_api_base
        api_key = settings.llm_primary_api_key
        api_kind = settings.llm_primary_api_kind

    provider = (provider or "").strip().lower()
    model = (model or "").strip()

    # 兼容旧写法：模型名前面又写了一遍提供商（`deepseek/deepseek-chat`）。
    # **只有当斜杠前的名字与配置的提供商相同**时才剥掉 ——
    # 这样 `provider=openrouter` + `model=anthropic/claude-sonnet-4`
    # 这种"模型名本身带斜杠"的正常配置不会被误伤。
    if provider and "/" in model:
        original = model
        head, rest = model.split("/", 1)
        if head.strip().lower() == provider and rest.strip():
            model = rest.strip()
            marker = (provider, model)
            if marker not in _legacy_warned:
                _legacy_warned.add(marker)
                logger.warning(
                    "模型名 %r 里又写了一遍提供商 —— 这是旧写法，已按"
                    "「提供商 + 模型名」解析成 %s / %s。建议拆成两个配置："
                    "PROVIDER=%s 与 MODEL=%s。",
                    original,
                    provider,
                    model,
                    provider,
                    model,
                )

    return LLMTarget(
        provider=provider,
        model=model,
        api_base=(api_base or "").strip(),
        api_key=(api_key or "").strip(),
        api_kind=(api_kind or "").strip().lower(),
    )
