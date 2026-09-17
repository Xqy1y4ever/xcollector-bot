"""网关的异常类型。

刻意分成两类，因为调用方对它们的处理**完全不同**：

  - `LLMConfigError`：配置问题（比如没设 API key）。重试一百次也没用，
    应该让用户去改配置。
  - `LLMError`：调用失败（超时、限流、厂商报错）。可以重试，也可以降级到
    规则抽取 —— 调用方靠 `status` 判断值不值得重试。

两者都继承 RuntimeError，所以 `except Exception` 的老代码不会漏掉它们。
"""

from __future__ import annotations


class LLMError(RuntimeError):
    """调用模型失败。"""

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        status: int | None = None,
        body: str | None = None,
    ):
        super().__init__(message)
        self.provider = provider
        self.model = model
        self.status = status
        # 厂商返回的原文，截断后留着排障。**不要直接展示给终端用户**。
        self.body = (body or "")[:500] or None

    @property
    def is_client_error(self) -> bool:
        """4xx：多半是请求本身的问题（比如厂商不支持 response_format）。"""
        return self.status is not None and 400 <= self.status < 500

    @property
    def is_retryable(self) -> bool:
        """5xx / 429 / 网络错误值得重试；其他 4xx 重试是浪费时间。"""
        if self.status is None:
            return True  # 网络层错误
        return self.status == 429 or self.status >= 500


class LLMConfigError(LLMError):
    """配置不完整 —— 重试无意义。"""
