"""LLM 网关层：一个极小的多厂商转接层。

为什么自己写而不用 litellm：我们只需要「用统一格式调几个厂商的 chat 接口」
这一件事，而 litellm 是「100+ 厂商的统一网关」，会拖进 openai / tokenizers /
tiktoken / aiohttp / jsonschema 等一整棵依赖树，镜像因此多出上百 MB。

这里只依赖 `httpx`（本来就是核心依赖），所以**没有新增任何依赖**。

支持两种协议：
  - **openai**：`POST {base}/chat/completions`，Bearer 鉴权。
    OpenAI 自己以及 DeepSeek / Moonshot / 智谱 / 通义 / OpenRouter / Groq /
    Ollama / 硅基流动 等兼容端点都走这条。
  - **google**：`POST {base}/models/{model}:generateContent`，原生 Gemini 格式
    （不是那个 OpenAI 兼容垫片）。请求体、消息结构、图片、用量字段都和 OpenAI
    不同，所以单独一条适配器。

模型写法：`厂商/模型名`，例如 `deepseek/deepseek-chat`、`google/gemini-2.5-flash`。
不写前缀时按 `openai` 处理。模型名里带斜杠也没问题（如
`openrouter/anthropic/claude-sonnet-4`），只有**第一段**被当作厂商。
"""

from .errors import LLMConfigError, LLMError
from .gateway import LLMResult, acompletion, close_client, resolve
from .providers import PROVIDERS, Provider

__all__ = [
    "acompletion",
    "close_client",
    "resolve",
    "LLMResult",
    "LLMError",
    "LLMConfigError",
    "PROVIDERS",
    "Provider",
]
