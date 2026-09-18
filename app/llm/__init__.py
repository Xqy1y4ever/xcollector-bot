"""LLM 网关层：一个极小的多厂商转接层。

只依赖 `httpx`（本来就是核心依赖），**没有新增任何依赖** —— 不引厂商 SDK。

支持两种协议：
  - **openai**：`POST {base}/chat/completions`，Bearer 鉴权。
    OpenAI 自己以及 DeepSeek / Moonshot / 智谱 / 通义 / OpenRouter / Groq /
    Ollama / 硅基流动 等兼容端点都走这条。
  - **google**：`POST {base}/models/{model}:generateContent`，原生 Gemini 格式
    （不是那个 OpenAI 兼容垫片）。请求体、消息结构、图片、用量字段都和 OpenAI
    不同，所以单独一条适配器。

**调用目标拆成「提供商 + 模型名」两段**（见 target.py）：
提供商决定协议和默认地址，模型名原样转发；想接没内置的端点就在
`api_base` / `api_key` / `api_kind` 里覆盖，不用改代码。
"""

from .errors import LLMConfigError, LLMError
from .gateway import LLMResult, acompletion, close_client
from .providers import PROVIDERS, Provider
from .target import LLMTarget, build_provider, target_from_settings

__all__ = [
    "acompletion",
    "close_client",
    "LLMTarget",
    "build_provider",
    "target_from_settings",
    "LLMResult",
    "LLMError",
    "LLMConfigError",
    "PROVIDERS",
    "Provider",
]
