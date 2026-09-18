"""提供商注册表。

每家只需要几个信息：叫什么、说哪种协议、默认 base URL 在哪、API key 读哪个环境变量。

这张表只是**默认值**，三种覆盖方式（都在 target.build_provider 里处理）：
  - 配置项 `LLM_*_API_BASE` / `LLM_*_API_KEY` / `LLM_*_API_KIND` —— 推荐，一处配完
  - 环境变量 `{提供商名大写}_API_BASE` —— 沿用旧写法
  - 提供商名字**不在这张表里** + 给了 `API_BASE` → 按 OpenAI 兼容端点处理
    （想用 Google 协议就把 `API_KIND` 设成 google）
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

Kind = Literal["openai", "google"]


@dataclass(frozen=True)
class Provider:
    name: str
    kind: Kind
    base_url: str
    # 按顺序找，第一个有值的生效。允许一个厂商有多个惯用变量名
    # （Google 就同时有 GEMINI_API_KEY 和 GOOGLE_API_KEY 两种写法）。
    key_envs: tuple[str, ...] = ()
    requires_key: bool = True
    extra_headers: tuple[tuple[str, str], ...] = field(default=())
    # 显式指定的 key（来自 LLM_*_API_KEY）。设了它就不再看环境变量 ——
    # 这是给自建 / 中转端点用的：一个 key 配在这一处，不用去记厂商的变量名。
    key_override: str = ""

    @property
    def base_env(self) -> str:
        return f"{self.name.upper().replace('-', '_')}_API_BASE"

    def resolved_base_url(self) -> str:
        return (os.environ.get(self.base_env) or self.base_url).rstrip("/")

    def api_key(self) -> str | None:
        if self.key_override:
            return self.key_override
        for env in self.key_envs:
            value = (os.environ.get(env) or "").strip()
            if value:
                return value
        return None

    @property
    def needs_env_key(self) -> bool:
        """是否**必须**从环境变量里拿 key。显式给了 key 就不必。"""
        return self.requires_key and not self.key_override


GOOGLE_BASE = "https://generativelanguage.googleapis.com/v1beta"
GOOGLE_KEYS = ("GEMINI_API_KEY", "GOOGLE_API_KEY")

PROVIDERS: dict[str, Provider] = {
    # ---- OpenAI 自己 ----
    "openai": Provider("openai", "openai", "https://api.openai.com/v1", ("OPENAI_API_KEY",)),
    # ---- 国内常见的 OpenAI 兼容端点 ----
    "deepseek": Provider("deepseek", "openai", "https://api.deepseek.com/v1", ("DEEPSEEK_API_KEY",)),
    "moonshot": Provider("moonshot", "openai", "https://api.moonshot.cn/v1", ("MOONSHOT_API_KEY",)),
    "zhipu": Provider(
        "zhipu", "openai", "https://open.bigmodel.cn/api/paas/v4",
        ("ZHIPUAI_API_KEY", "ZHIPU_API_KEY"),
    ),
    "dashscope": Provider(
        "dashscope", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
        ("DASHSCOPE_API_KEY",),
    ),
    "siliconflow": Provider(
        "siliconflow", "openai", "https://api.siliconflow.cn/v1", ("SILICONFLOW_API_KEY",)
    ),
    "volcengine": Provider(
        "volcengine", "openai", "https://ark.cn-beijing.volces.com/api/v3",
        ("ARK_API_KEY",),
    ),
    # ---- 国外常见的 OpenAI 兼容端点 ----
    "openrouter": Provider(
        "openrouter", "openai", "https://openrouter.ai/api/v1", ("OPENROUTER_API_KEY",)
    ),
    "groq": Provider("groq", "openai", "https://api.groq.com/openai/v1", ("GROQ_API_KEY",)),
    "mistral": Provider("mistral", "openai", "https://api.mistral.ai/v1", ("MISTRAL_API_KEY",)),
    "xai": Provider("xai", "openai", "https://api.x.ai/v1", ("XAI_API_KEY",)),
    # ---- 本地，不需要 key ----
    "ollama": Provider(
        "ollama", "openai", "http://127.0.0.1:11434/v1", (), requires_key=False
    ),
    "vllm": Provider("vllm", "openai", "http://127.0.0.1:8000/v1", (), requires_key=False),
    # ---- Google：两种都留着 ----
    # 原生 generateContent（默认，功能最全）
    "google": Provider("google", "google", GOOGLE_BASE, GOOGLE_KEYS),
    "gemini": Provider("google", "google", GOOGLE_BASE, GOOGLE_KEYS),
    # 官方提供的 OpenAI 兼容垫片。原生接口出问题时可以换这条兜底。
    "gemini-openai": Provider(
        "gemini-openai", "openai", f"{GOOGLE_BASE}/openai", GOOGLE_KEYS
    ),
}
