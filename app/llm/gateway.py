"""网关本体：两种协议各一个适配器，对外只暴露 `acompletion()`。

设计原则（和整个 bot 一致）：
  - **单次尝试，不做重试**。重试策略在 `pipeline/extract.py::run_llm` 里，
    那里知道"第一次用 JSON 模式、失败后退回普通模式"这种业务规则。
    网关重试会和它打架，还会让超时失控。
  - **错误带上 `status`**，让调用方能判断值不值得重试（见 errors.py）。
  - **不猜**。缺 key 就明确报缺哪个环境变量，而不是等 HTTP 401 再猜。
  - **图片降级要留痕**：Google 收不了任意 http 图片 URL，此时退化成一句
    占位文本并打 WARNING，而不是悄悄把图丢掉 —— 悄悄丢图就是悄悄漏 DDL。

用量字段两家名字不同，统一成 `LLMResult`：
  - OpenAI：`usage.total_tokens` / `prompt_tokens` / `completion_tokens`
  - Google：`usageMetadata.totalTokenCount` / `promptTokenCount` / `candidatesTokenCount`
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from .errors import LLMConfigError, LLMError
from .providers import PROVIDERS, Provider

logger = logging.getLogger(__name__)

# 单例 client：复用连接池。超时按请求传，因为每个调用点可能不同。
_CLIENT: httpx.AsyncClient | None = None


def _client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = httpx.AsyncClient(follow_redirects=True)
    return _CLIENT


async def close_client() -> None:
    """服务关停时调用，让连接池干净退出。"""
    global _CLIENT
    if _CLIENT is not None and not _CLIENT.is_closed:
        await _CLIENT.aclose()
    _CLIENT = None


@dataclass
class LLMResult:
    text: str
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    model: str = ""
    provider: str = ""
    finish_reason: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------
# 模型名解析
# --------------------------------------------------------------------------

def resolve(model: str) -> tuple[Provider, str]:
    """把 `厂商/模型名` 拆成 (Provider, 真正的模型名)。

    没写前缀 → 当作 openai。前缀没注册但设了 `{前缀}_API_BASE`
    → 当作自定义的 OpenAI 兼容端点（这样用户可以接任何自建服务，
    不需要改这个文件）。
    """
    raw = (model or "").strip()
    if not raw:
        raise LLMConfigError("模型名不能为空，应形如 deepseek/deepseek-chat")

    if "/" in raw:
        prefix, rest = raw.split("/", 1)
    else:
        prefix, rest = "openai", raw

    prefix = prefix.strip().lower()
    rest = rest.strip()
    if not rest:
        raise LLMConfigError(f"模型名不完整：{model!r}")

    provider = PROVIDERS.get(prefix)
    if provider is None:
        base_env = f"{prefix.upper().replace('-', '_')}_API_BASE"
        base = (os.environ.get(base_env) or "").strip()
        if not base:
            known = ", ".join(sorted(PROVIDERS))
            raise LLMConfigError(
                f"未知的模型厂商前缀 {prefix!r}。已内置：{known}。"
                f"如果这是自建的 OpenAI 兼容服务，请设环境变量 {base_env}。"
            )
        provider = Provider(
            prefix, "openai", base, (f"{prefix.upper().replace('-', '_')}_API_KEY",),
            requires_key=False,
        )
    return provider, rest


def _api_key(provider: Provider) -> str | None:
    key = provider.api_key()
    if key is None and provider.requires_key:
        wanted = " 或 ".join(provider.key_envs) or "(未配置环境变量名)"
        raise LLMConfigError(
            f"厂商 {provider.name!r} 缺少 API key，请设置环境变量：{wanted}"
        )
    return key


# --------------------------------------------------------------------------
# 对外入口
# --------------------------------------------------------------------------

async def acompletion(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float = 0.0,
    timeout: float = 60.0,
    response_format: dict[str, Any] | None = None,
    max_tokens: int | None = None,
    json_mode: bool | None = None,
) -> LLMResult:
    """调一次模型。

    `response_format={"type": "json_object"}` 在 OpenAI 侧原样透传，
    在 Google 侧翻译成 `generationConfig.responseMimeType=application/json`。
    厂商不支持时抛 `LLMError`（`is_client_error` 为真），调用方据此降级重试。

    `json_mode` 是上面那个的简写，两者给一个就行。
    """
    if json_mode is not None and response_format is None:
        response_format = {"type": "json_object"} if json_mode else None

    provider, model_name = resolve(model)
    key = _api_key(provider)
    url, headers, body = _build_request(
        provider, model_name, key, messages, temperature, response_format, max_tokens
    )

    try:
        resp = await _client().post(url, headers=headers, json=body, timeout=timeout)
    except httpx.TimeoutException as exc:
        raise LLMError(
            f"调用 {provider.name} 超时（{timeout}s，model={model_name}）",
            provider=provider.name, model=model_name,
        ) from exc
    except httpx.HTTPError as exc:
        raise LLMError(
            f"连接 {provider.name} 失败：{exc}",
            provider=provider.name, model=model_name,
        ) from exc

    if resp.status_code >= 400:
        snippet = resp.text[:500]
        hint = ""
        if resp.status_code == 401:
            hint = "（API key 无效或已过期）"
        elif resp.status_code == 403:
            hint = "（鉴权通过但无权限，可能是模型未开通）"
        elif resp.status_code == 429:
            hint = "（限流或余额不足）"
        elif resp.status_code == 404:
            hint = "（模型名或 base URL 不对）"
        raise LLMError(
            f"{provider.name} 返回 HTTP {resp.status_code}{hint}：{snippet}",
            provider=provider.name, model=model_name,
            status=resp.status_code, body=snippet,
        )

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError) as exc:
        raise LLMError(
            f"{provider.name} 返回的不是 JSON（前 200 字）：{resp.text[:200]}",
            provider=provider.name, model=model_name, status=resp.status_code,
        ) from exc

    if provider.kind == "google":
        return _parse_google(data, provider, model_name)
    return _parse_openai(data, provider, model_name)


# --------------------------------------------------------------------------
# 请求构造
# --------------------------------------------------------------------------

def _build_request(
    provider: Provider,
    model: str,
    key: str | None,
    messages: list[dict[str, Any]],
    temperature: float,
    response_format: dict[str, Any] | None,
    max_tokens: int | None,
) -> tuple[str, dict[str, str], dict[str, Any]]:
    base = provider.resolved_base_url()
    headers = {"Content-Type": "application/json"}
    for name, value in provider.extra_headers:
        headers[name] = value

    if provider.kind == "google":
        # 刻意把 key 放 header 而不是 URL 的 ?key= 参数：URL 会出现在
        # 异常信息、日志和反代访问日志里，header 不会。
        headers["x-goog-api-key"] = key or ""
        system_parts, contents = _to_google_messages(messages)
        generation: dict[str, Any] = {"temperature": temperature}
        if max_tokens:
            generation["maxOutputTokens"] = max_tokens
        if response_format and response_format.get("type") == "json_object":
            generation["responseMimeType"] = "application/json"
        body: dict[str, Any] = {"contents": contents, "generationConfig": generation}
        if system_parts:
            body["systemInstruction"] = {"parts": system_parts}
        return f"{base}/models/{model}:generateContent", headers, body

    if key:
        headers["Authorization"] = f"Bearer {key}"
    body = {"model": model, "messages": messages, "temperature": temperature}
    if response_format:
        body["response_format"] = response_format
    if max_tokens:
        body["max_tokens"] = max_tokens
    return f"{base}/chat/completions", headers, body


_DATA_URL_RE = re.compile(r"^data:(?P<mime>[\w.+-]+/[\w.+-]+)?;base64,(?P<data>.+)$", re.S)


def _to_google_messages(
    messages: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """OpenAI 风格 messages → Google 的 (systemInstruction parts, contents)。

    Google 没有 system role，系统提示要单独放进 `systemInstruction`。
    role 名也不同：`assistant` → `model`。
    """
    system_parts: list[dict[str, Any]] = []
    contents: list[dict[str, Any]] = []

    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")
        if role == "system":
            system_parts.extend(_to_google_parts(content))
            continue
        google_role = "model" if role == "assistant" else "user"
        parts = _to_google_parts(content)
        if not parts:
            continue
        # 连续同 role 会触发 Google 的 400，合并掉。
        if contents and contents[-1]["role"] == google_role:
            contents[-1]["parts"].extend(parts)
        else:
            contents.append({"role": google_role, "parts": parts})

    if not contents:
        # Google 要求 contents 非空；纯 system 的请求按空用户消息处理。
        contents = [{"role": "user", "parts": [{"text": ""}]}]
    return system_parts, contents


def _to_google_parts(content: Any) -> list[dict[str, Any]]:
    if content is None:
        return []
    if isinstance(content, str):
        return [{"text": content}] if content else []

    parts: list[dict[str, Any]] = []
    for part in content:
        if not isinstance(part, dict):
            parts.append({"text": str(part)})
            continue
        kind = part.get("type")
        if kind == "text":
            text = part.get("text") or ""
            if text:
                parts.append({"text": text})
        elif kind == "image_url":
            parts.append(_google_image_part((part.get("image_url") or {}).get("url") or ""))
        else:
            # 未知类型：保留成文本，别静默丢内容。
            parts.append({"text": json.dumps(part, ensure_ascii=False)})
    return parts


def _google_image_part(url: str) -> dict[str, Any]:
    match = _DATA_URL_RE.match(url.strip())
    if match:
        mime = match.group("mime") or "image/jpeg"
        return {"inlineData": {"mimeType": mime, "data": match.group("data")}}

    # Google 不接受任意 http 图片 URL（fileData 只认 gs:// 和上传后的文件 URI）。
    # 这里退化成占位文本而不是抛错：宁可少一张图，也不能让整条消息抽取不出来。
    # 但必须打 WARNING —— 否则就是"静默漏信息"。
    logger.warning(
        "Google 原生接口无法内联非 data URL 的图片，本条消息的这张图被跳过：%.120s", url
    )
    return {"text": "（本应有一张图片，但当前厂商接口无法读取该图片地址，已跳过）"}


# --------------------------------------------------------------------------
# 响应解析
# --------------------------------------------------------------------------

def _parse_openai(data: dict[str, Any], provider: Provider, model: str) -> LLMResult:
    choices = data.get("choices") or []
    if not choices:
        # 有的兼容端点出错时 HTTP 200 但 body 里带 error。
        err = data.get("error")
        if err:
            detail = err.get("message") if isinstance(err, dict) else str(err)
            raise LLMError(
                f"{provider.name} 返回错误：{detail}",
                provider=provider.name, model=model, body=json.dumps(data, ensure_ascii=False),
            )
        raise LLMError(
            f"{provider.name} 返回结果里没有 choices",
            provider=provider.name, model=model, body=json.dumps(data, ensure_ascii=False),
        )

    choice = choices[0] or {}
    message = choice.get("message") or {}
    text = _content_to_text(message.get("content"))
    if not text:
        # 推理模型有时把答案放 reasoning_content，或只给了 tool_calls。
        text = _content_to_text(message.get("reasoning_content"))
    if not text:
        text = _content_to_text(choice.get("text"))

    usage = data.get("usage") or {}
    prompt = _int(usage.get("prompt_tokens"))
    completion = _int(usage.get("completion_tokens"))
    total = _int(usage.get("total_tokens")) or (prompt + completion)

    return LLMResult(
        text=text,
        total_tokens=total,
        prompt_tokens=prompt,
        completion_tokens=completion,
        model=data.get("model") or model,
        provider=provider.name,
        finish_reason=choice.get("finish_reason"),
        raw=data,
    )


def _parse_google(data: dict[str, Any], provider: Provider, model: str) -> LLMResult:
    candidates = data.get("candidates") or []
    if not candidates:
        # 安全策略拦截时没有 candidates，但 promptFeedback 会说明原因。
        feedback = data.get("promptFeedback") or {}
        reason = feedback.get("blockReason")
        err = data.get("error")
        if err:
            detail = err.get("message") if isinstance(err, dict) else str(err)
            raise LLMError(
                f"{provider.name} 返回错误：{detail}",
                provider=provider.name, model=model, body=json.dumps(data, ensure_ascii=False),
            )
        if reason:
            raise LLMError(
                f"{provider.name} 因安全策略拦截了本次请求（blockReason={reason}）",
                provider=provider.name, model=model, body=json.dumps(data, ensure_ascii=False),
            )
        raise LLMError(
            f"{provider.name} 返回结果里没有 candidates",
            provider=provider.name, model=model, body=json.dumps(data, ensure_ascii=False),
        )

    candidate = candidates[0] or {}
    parts = ((candidate.get("content") or {}).get("parts")) or []
    text = "".join(p.get("text") or "" for p in parts if isinstance(p, dict))

    usage = data.get("usageMetadata") or {}
    prompt = _int(usage.get("promptTokenCount"))
    completion = _int(usage.get("candidatesTokenCount"))
    total = _int(usage.get("totalTokenCount")) or (prompt + completion)

    return LLMResult(
        text=text,
        total_tokens=total,
        prompt_tokens=prompt,
        completion_tokens=completion,
        model=data.get("modelVersion") or model,
        provider=provider.name,
        finish_reason=candidate.get("finishReason"),
        raw=data,
    )


def _content_to_text(content: Any) -> str:
    """OpenAI 的 content 可能是字符串，也可能是 parts 数组。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            (p.get("text") or "")
            for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        )
    return str(content)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
