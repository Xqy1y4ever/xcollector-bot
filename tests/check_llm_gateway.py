"""LLM 网关回归测试。

    python -m tests.check_llm_gateway

这里的重点是**线上格式本身**：URL 拼得对不对、鉴权头对不对、请求体的字段
对不对、两家的响应怎么统一。所以不用 mock，而是起一个真的本地 HTTP 服务，
让 httpx 真的发一次 HTTP 请求 —— mock 掉 transport 就测不出 URL 和 header 了。

覆盖两条协议（OpenAI / Google 原生）、错误映射、图片内联、以及
"配置写错要明确报错而不是猜"。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.llm import LLMConfigError, LLMError, LLMTarget, acompletion, build_provider
from app.llm.target import target_from_settings
from app.llm.providers import PROVIDERS

# --------------------------------------------------------------------------
# 假厂商服务
# --------------------------------------------------------------------------

_STATE: dict = {"requests": [], "responder": None}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"__raw__": raw.decode("utf-8", "replace")}
        _STATE["requests"].append(
            {
                "path": self.path,
                # HTTP header 名是大小写不敏感的，而且 httpx 会把它规范化成
                # X-Goog-Api-Key 这种形式，所以统一小写再存，断言才有意义。
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": parsed,
            }
        )
        status, payload = _STATE["responder"](self.path)
        if isinstance(payload, str):
            body = payload.encode()
        else:
            body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:  # 别刷屏
        pass


def _start_server() -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{port}"


# --------------------------------------------------------------------------
# 断言工具
# --------------------------------------------------------------------------

_FAILURES: list[str] = []
_CHECKS = 0


def check(cond: bool, label: str, detail: str = "") -> None:
    global _CHECKS
    _CHECKS += 1
    if cond:
        return
    _FAILURES.append(f"{label}{(' —— ' + detail) if detail else ''}")
    print(f"FAIL  {label}")
    if detail:
        print(f"      {detail}")


def eq(got, want, label: str) -> None:
    check(got == want, label, f"期望 {want!r}，实际 {got!r}")


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


# --------------------------------------------------------------------------
# 响应样例
# --------------------------------------------------------------------------

def openai_response(text: str = '{"ok":true}') -> dict:
    return {
        "id": "chatcmpl-1",
        "model": "deepseek-chat",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
    }


def google_response(text: str = '{"ok":true}') -> dict:
    return {
        "candidates": [
            {
                "content": {"role": "model", "parts": [{"text": text}]},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 13,
            "candidatesTokenCount": 5,
            "totalTokenCount": 18,
        },
        "modelVersion": "gemini-2.5-flash",
    }


# --------------------------------------------------------------------------
# 用例
# --------------------------------------------------------------------------

def test_build_provider() -> None:
    """提供商解析 + 显式覆盖（base / key / 协议）。"""
    p = build_provider("deepseek")
    eq(p.name, "deepseek", "provider: 名字")
    eq(p.kind, "openai", "provider: deepseek 走 openai 协议")

    p = build_provider("GOOGLE")  # 大小写不敏感
    eq(p.kind, "google", "provider: google 走 google 协议")

    p = build_provider("gemini")
    eq(p.name, "google", "provider: gemini 是 google 的别名")
    eq(p.kind, "google", "provider: 别名也用 google 协议")

    # 空名字要报配置错误，并说清楚该配什么
    for bad in ("", "   "):
        try:
            build_provider(bad)
            check(False, f"provider: {bad!r} 应当报错")
        except LLMConfigError as exc:
            check("LLM_PRIMARY_PROVIDER" in str(exc), f"provider: {bad!r} 的报错给出该配哪一项", str(exc))

    # 未知提供商且没给 base → 明确报错，并告诉他自建端点该填什么
    try:
        build_provider("mystery")
        check(False, "provider: 未知提供商且无 base 应当报错")
    except LLMConfigError as exc:
        check("API_BASE" in str(exc), "provider: 未知提供商的报错里提到 API_BASE", str(exc))

    # 未知提供商 + base → 按 OpenAI 兼容端点处理，且不强制要 key
    p = build_provider("mystery", api_base="http://127.0.0.1:1/v1")
    eq(p.kind, "openai", "provider: 自建端点默认按 openai 协议")
    eq(p.resolved_base_url(), "http://127.0.0.1:1/v1", "provider: 自建端点用给定的 base")
    check(not p.requires_key, "provider: 自建端点不强制要 key")

    # 未知提供商 + base + kind=google → 走 Google 原生协议（想用哪个端点都可以）
    p = build_provider("mygemini", api_base="http://127.0.0.1:2/v1beta", api_kind="google")
    eq(p.kind, "google", "provider: 自建端点可以指定 google 协议")

    # 非法协议要报错，不能静默按 openai 处理
    try:
        build_provider("mystery", api_base="http://x/v1", api_kind="anthropic")
        check(False, "provider: 非法 API_KIND 应当报错")
    except LLMConfigError as exc:
        check("openai 或 google" in str(exc), "provider: 非法 API_KIND 的报错说明合法取值", str(exc))

    # 内置提供商也能被覆盖 base / key
    p = build_provider("openai", api_base="http://proxy.example.com/v1")
    eq(p.resolved_base_url(), "http://proxy.example.com/v1", "provider: 内置厂商的 base 可覆盖")
    eq(p.key_envs, PROVIDERS["openai"].key_envs, "provider: 覆盖 base 不影响 key 变量名")

    p = build_provider("openai", api_key="sk-explicit")
    eq(p.api_key(), "sk-explicit", "provider: 显式 key 生效")
    check(not p.needs_env_key, "provider: 给了显式 key 就不再要求环境变量")


def test_target_from_settings() -> None:
    """配置 → 调用目标，含旧写法的兼容。"""
    from app.config import Settings

    s = Settings(
        llm_primary_provider="openrouter",
        llm_primary_model="anthropic/claude-sonnet-4",
    )
    t = target_from_settings(s, "primary")
    eq(t.provider, "openrouter", "target: 提供商")
    # 关键：模型名里带斜杠**不能**被当成"又写了一遍提供商"
    eq(t.model, "anthropic/claude-sonnet-4", "target: 模型名里的斜杠原样保留")
    eq(t.label, "openrouter/anthropic/claude-sonnet-4", "target: label")

    # 旧写法：模型名前面又写了一遍提供商 → 剥掉，仍然可用
    s = Settings(llm_primary_provider="deepseek", llm_primary_model="deepseek/deepseek-chat")
    t = target_from_settings(s, "primary")
    eq(t.model, "deepseek-chat", "target: 旧写法里重复的提供商前缀被剥掉")
    eq(t.label, "deepseek/deepseek-chat", "target: label 与旧的单字符串写法一致")

    # 覆盖项透传
    s = Settings(
        llm_primary_provider="myproxy",
        llm_primary_model="qwen3-32b",
        llm_primary_api_base="https://llm.corp.example.com/v1",
        llm_primary_api_key="k-123",
        llm_primary_api_kind="openai",
    )
    t = target_from_settings(s, "primary")
    eq(t.api_base, "https://llm.corp.example.com/v1", "target: api_base 透传")
    eq(t.api_key, "k-123", "target: api_key 透传")
    eq(t.api_kind, "openai", "target: api_kind 透传")
    check(t.is_configured, "target: 配全了")
    check("k-123" not in t.describe(), "target: describe 不泄露 key", t.describe())

    # 次模型走另一套字段
    s = Settings(
        llm_primary_provider="deepseek",
        llm_primary_model="deepseek-chat",
        llm_secondary_provider="google",
        llm_secondary_model="gemini-2.5-flash",
    )
    eq(target_from_settings(s, "secondary").provider, "google", "target: 次模型用 secondary 字段")
    check(s.cross_check_enabled, "target: 次模型配了就开交叉验证")

    # 只差提供商也算不同模型
    s = Settings(
        llm_primary_provider="deepseek",
        llm_primary_model="deepseek-chat",
        llm_secondary_provider="openrouter",
        llm_secondary_model="deepseek-chat",
    )
    check(s.cross_check_enabled, "target: 提供商不同也算不同模型，交叉验证仍然有意义")

    # 完全没配 / 模型名为空 → is_configured 为假，调用时报明确错误
    s = Settings(llm_primary_provider="", llm_primary_model="")
    check(not target_from_settings(s, "primary").is_configured, "target: 空配置识别为未配置")


def test_missing_key() -> None:
    for env in PROVIDERS["openai"].key_envs:
        os.environ.pop(env, None)
    try:
        asyncio.run(
            acompletion(target=LLMTarget(provider="openai", model="gpt-4o-mini"), messages=[{"role": "user", "content": "hi"}])
        )
        check(False, "缺 key 应当报 LLMConfigError")
    except LLMConfigError as exc:
        check("OPENAI_API_KEY" in str(exc), "缺 key 的报错里点名环境变量", str(exc))
    except Exception as exc:  # noqa: BLE001
        check(False, "缺 key 应当报 LLMConfigError 而不是别的", repr(exc))


def test_openai_wire(base: str) -> None:
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = f"{base}/v1"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, openai_response())
    logging.getLogger("app.llm.gateway").setLevel(logging.WARNING)

    result = asyncio.run(
        acompletion(
            target=LLMTarget(provider="openai", model="deepseek-chat"),
            messages=[
                {"role": "system", "content": "你是抽取助手"},
                {"role": "user", "content": "明天中午12点前交表"},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=512,
        )
    )

    eq(len(_STATE["requests"]), 1, "openai: 只发一次请求")
    req = _STATE["requests"][0]
    eq(req["path"], "/v1/chat/completions", "openai: URL 路径")
    eq(req["headers"].get("authorization"), "Bearer sk-test", "openai: 鉴权头")
    eq(req["body"]["model"], "deepseek-chat", "openai: 请求体带模型名")
    eq(req["body"]["temperature"], 0.0, "openai: 请求体带 temperature")
    eq(req["body"]["response_format"], {"type": "json_object"}, "openai: JSON 模式原样透传")
    eq(req["body"]["max_tokens"], 512, "openai: max_tokens")
    eq(len(req["body"]["messages"]), 2, "openai: 消息条数")
    eq(req["body"]["messages"][0]["role"], "system", "openai: system 消息原样保留")

    eq(result.text, '{"ok":true}', "openai: 解析出文本")
    eq(result.total_tokens, 18, "openai: total_tokens")
    eq(result.prompt_tokens, 11, "openai: prompt_tokens")
    eq(result.completion_tokens, 7, "openai: completion_tokens")
    eq(result.provider, "openai", "openai: provider 回填")
    eq(result.finish_reason, "stop", "openai: finish_reason")


def test_openai_image_passthrough(base: str) -> None:
    """OpenAI 协议下图片就是 image_url + data URL，必须原样透传。"""
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = f"{base}/v1"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, openai_response("{}"))

    data_url = "data:image/png;base64,QUJD"
    asyncio.run(
        acompletion(
            target=LLMTarget(provider="openai", model="gpt-4o-mini"),
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "看这张图"},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
        )
    )
    parts = _STATE["requests"][0]["body"]["messages"][0]["content"]
    eq(parts[1]["type"], "image_url", "openai: 图片 part 类型不变")
    eq(parts[1]["image_url"]["url"], data_url, "openai: 图片 data URL 原样透传")


def test_google_wire(base: str) -> None:
    os.environ["GEMINI_API_KEY"] = "gm-test"
    os.environ["GOOGLE_API_BASE"] = f"{base}/v1beta"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, google_response())
    logging.getLogger("app.llm.gateway").setLevel(logging.WARNING)

    data_url = "data:image/jpeg;base64,QUJD"
    result = asyncio.run(
        acompletion(
            target=LLMTarget(provider="google", model="gemini-2.5-flash"),
            messages=[
                {"role": "system", "content": "你是抽取助手"},
                {"role": "user", "content": "明天中午12点前交表"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "看这张图"},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
                {"role": "assistant", "content": "好的"},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
            max_tokens=256,
        )
    )

    eq(len(_STATE["requests"]), 1, "google: 只发一次请求")
    req = _STATE["requests"][0]
    eq(req["path"], "/v1beta/models/gemini-2.5-flash:generateContent", "google: URL 路径")
    eq(req["headers"].get("x-goog-api-key"), "gm-test", "google: 用 x-goog-api-key 鉴权")
    check(
        "authorization" not in req["headers"],
        "google: 不应带 Bearer 鉴权头",
        str(req["headers"].get("authorization")),
    )

    body = req["body"]
    eq(
        body["systemInstruction"]["parts"][0]["text"],
        "你是抽取助手",
        "google: system 抽出来放进 systemInstruction",
    )
    eq(
        body["generationConfig"]["responseMimeType"],
        "application/json",
        "google: JSON 模式翻译成 responseMimeType",
    )
    eq(body["generationConfig"]["temperature"], 0.2, "google: temperature")
    eq(body["generationConfig"]["maxOutputTokens"], 256, "google: maxOutputTokens")

    # 两条连续的 user 必须合并（Google 不接受同角色相邻）
    eq(len(body["contents"]), 2, "google: 连续同角色被合并")
    eq(body["contents"][0]["role"], "user", "google: 第一条是 user")
    eq(body["contents"][1]["role"], "model", "google: assistant 翻译成 model")
    parts = body["contents"][0]["parts"]
    eq(parts[0]["text"], "明天中午12点前交表", "google: 文本 part")
    eq(parts[1]["text"], "看这张图", "google: 合并后的文本 part")
    eq(
        parts[2]["inlineData"],
        {"mimeType": "image/jpeg", "data": "QUJD"},
        "google: 图片 data URL 转成 inlineData",
    )
    check("image_url" not in json.dumps(body), "google: 请求体里不应残留 OpenAI 风格字段")

    eq(result.text, '{"ok":true}', "google: 解析出文本")
    eq(result.total_tokens, 18, "google: 从 usageMetadata 取 totalTokenCount")
    eq(result.prompt_tokens, 13, "google: promptTokenCount")
    eq(result.completion_tokens, 5, "google: candidatesTokenCount")
    eq(result.model, "gemini-2.5-flash", "google: modelVersion 回填")
    eq(result.finish_reason, "STOP", "google: finishReason")


def test_google_multi_part_text(base: str) -> None:
    """Google 会把答案拆成多个 part，必须拼起来而不是只取第一个。"""
    os.environ["GEMINI_API_KEY"] = "gm-test"
    os.environ["GOOGLE_API_BASE"] = f"{base}/v1beta"
    _STATE["responder"] = lambda path: (
        200,
        {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [{"text": '{"title":'}, {"text": '"班会"}'}],
                    },
                    "finishReason": "STOP",
                }
            ],
            "usageMetadata": {"totalTokenCount": 3},
        },
    )
    result = asyncio.run(
        acompletion(target=LLMTarget(provider="google", model="gemini-2.5-flash"), messages=[{"role": "user", "content": "x"}])
    )
    eq(result.text, '{"title":"班会"}', "google: 多个 text part 拼接")
    eq(result.total_tokens, 3, "google: 只有 totalTokenCount 时也能取到")


def test_google_http_image_degrades(base: str) -> None:
    """Google 读不了任意 http 图片：要降级成占位文本 + 打 WARNING，不能抛错、不能静默丢。"""
    os.environ["GEMINI_API_KEY"] = "gm-test"
    os.environ["GOOGLE_API_BASE"] = f"{base}/v1beta"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, google_response("{}"))

    logger = logging.getLogger("app.llm.gateway")
    capture = _Capture()
    old_level = logger.level
    logger.addHandler(capture)
    logger.setLevel(logging.WARNING)
    try:
        asyncio.run(
            acompletion(
                target=LLMTarget(provider="google", model="gemini-2.5-flash"),
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "看这张图"},
                            {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
                        ],
                    }
                ],
            )
        )
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)

    parts = _STATE["requests"][0]["body"]["contents"][0]["parts"]
    eq(parts[0]["text"], "看这张图", "google: 无法内联时文本部分必须保留")
    check(
        "inlineData" not in parts[1] and "text" in parts[1],
        "google: http 图片降级为占位文本",
        json.dumps(parts[1], ensure_ascii=False),
    )
    check(
        any("https://example.com/a.png" in r.getMessage() for r in capture.records),
        "google: 图片被跳过时必须打 WARNING 留痕",
        f"实际日志：{[r.getMessage() for r in capture.records]}",
    )


def test_no_text_when_blocked(base: str) -> None:
    """Google 安全拦截：没有 candidates 但要给出明确原因。"""
    os.environ["GEMINI_API_KEY"] = "gm-test"
    os.environ["GOOGLE_API_BASE"] = f"{base}/v1beta"
    _STATE["responder"] = lambda path: (
        200,
        {"promptFeedback": {"blockReason": "SAFETY"}, "usageMetadata": {}},
    )
    try:
        asyncio.run(
            acompletion(target=LLMTarget(provider="google", model="gemini-2.5-flash"), messages=[{"role": "user", "content": "x"}])
        )
        check(False, "google: 被拦截时应当报错")
    except LLMError as exc:
        check("SAFETY" in str(exc), "google: 报错里带 blockReason", str(exc))


def test_errors(base: str) -> None:
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = f"{base}/v1"

    cases = [
        (401, {"error": {"message": "bad key"}}, True, False, "401"),
        (403, {"error": {"message": "no access"}}, True, False, "403"),
        (404, {"error": {"message": "no model"}}, True, False, "404"),
        (429, {"error": {"message": "rate limit"}}, True, True, "429"),
        (500, {"error": {"message": "boom"}}, False, True, "500"),
        (503, {"error": {"message": "down"}}, False, True, "503"),
    ]
    for status, payload, want_client, want_retry, label in cases:
        _STATE["responder"] = lambda path, s=status, p=payload: (s, p)
        try:
            asyncio.run(
                acompletion(target=LLMTarget(provider="openai", model="gpt-4o-mini"), messages=[{"role": "user", "content": "x"}])
            )
            check(False, f"HTTP {label} 应当抛 LLMError")
        except LLMError as exc:
            eq(exc.status, status, f"HTTP {label}: 异常里带 status")
            eq(exc.is_client_error, want_client, f"HTTP {label}: is_client_error")
            eq(exc.is_retryable, want_retry, f"HTTP {label}: is_retryable")
            check("sk-test" not in str(exc), f"HTTP {label}: 报错里不能漏 API key", str(exc))

    # 200 但 body 里带 error
    _STATE["responder"] = lambda path: (200, {"error": {"message": "quota exceeded"}})
    try:
        asyncio.run(
            acompletion(target=LLMTarget(provider="openai", model="gpt-4o-mini"), messages=[{"role": "user", "content": "x"}])
        )
        check(False, "200 + body.error 应当报错")
    except LLMError as exc:
        check("quota exceeded" in str(exc), "200 + body.error 也要抛错", str(exc))

    # 200 但没有 choices
    _STATE["responder"] = lambda path: (200, {"usage": {}})
    try:
        asyncio.run(
            acompletion(target=LLMTarget(provider="openai", model="gpt-4o-mini"), messages=[{"role": "user", "content": "x"}])
        )
        check(False, "缺 choices 应当报错")
    except LLMError as exc:
        check("choices" in str(exc), "缺 choices 的报错可读", str(exc))

    # 200 但返回 HTML（反代打错了很常见）
    _STATE["responder"] = lambda path: (200, "<html>502 Bad Gateway</html>")
    try:
        asyncio.run(
            acompletion(target=LLMTarget(provider="openai", model="gpt-4o-mini"), messages=[{"role": "user", "content": "x"}])
        )
        check(False, "非 JSON 响应应当报错")
    except LLMError as exc:
        check("502 Bad Gateway" in str(exc), "非 JSON 响应把原文带进报错", str(exc))


def test_timeout(base: str) -> None:
    """超时必须是 LLMError 且 is_retryable，让上层知道可以再试。"""
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = "http://127.0.0.1:9/v1"  # 丢弃端口，必然连不上
    try:
        asyncio.run(
            acompletion(
                target=LLMTarget(provider="openai", model="gpt-4o-mini"),
                messages=[{"role": "user", "content": "x"}],
                timeout=1.0,
            )
        )
        check(False, "连不上时应当报错")
    except LLMError as exc:
        check(exc.is_retryable, "网络错误算可重试", str(exc))
        eq(exc.status, None, "网络错误没有 HTTP status")
        check("openai" in str(exc), "网络错误的报错里带厂商名", str(exc))


def test_json_mode_shorthand(base: str) -> None:
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = f"{base}/v1"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, openai_response("{}"))

    asyncio.run(
        acompletion(
            target=LLMTarget(provider="openai", model="gpt-4o-mini"),
            messages=[{"role": "user", "content": "x"}],
            json_mode=True,
        )
    )
    eq(
        _STATE["requests"][0]["body"]["response_format"],
        {"type": "json_object"},
        "json_mode=True 等价于 response_format",
    )

    _STATE["requests"].clear()
    asyncio.run(
        acompletion(
            target=LLMTarget(provider="openai", model="gpt-4o-mini"),
            messages=[{"role": "user", "content": "x"}],
            json_mode=False,
        )
    )
    check(
        "response_format" not in _STATE["requests"][0]["body"],
        "json_mode=False 时不带 response_format",
    )


def test_base_url_override(base: str) -> None:
    """{厂商}_API_BASE 覆盖要生效，且结尾斜杠不能拼出双斜杠。"""
    os.environ["DEEPSEEK_API_KEY"] = "ds-test"
    os.environ["DEEPSEEK_API_BASE"] = f"{base}/custom/v1/"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, openai_response("{}"))

    asyncio.run(
        acompletion(target=LLMTarget(provider="deepseek", model="deepseek-chat"), messages=[{"role": "user", "content": "x"}])
    )
    eq(_STATE["requests"][0]["path"], "/custom/v1/chat/completions", "base 覆盖 + 去掉尾部斜杠")


def test_across_event_loops(base: str) -> None:
    """同一个进程里跨多个事件循环调用。

    网关缓存了一个 httpx.AsyncClient 做连接池，而连接池绑定在创建它的**事件循环**上。
    如果只缓存 client 不缓存循环，第二次 asyncio.run() 会拿到绑定在已关闭循环上的
    连接，一用就炸 `RuntimeError: Event loop is closed` —— 而且是从连接池清理里抛出来的，
    堆栈跟业务代码毫无关系。

    这个文件里每个用例都是一次 asyncio.run，本来就在走这条路，但那是隐式的：
    只有池里恰好留着 keep-alive 连接时才炸（所以曾经偶发失败过）。这里显式写一条，
    连续三次、每次一个新循环，把不变式钉住。
    """
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["OPENAI_API_BASE"] = f"{base}/v1"
    _STATE["requests"].clear()
    _STATE["responder"] = lambda path: (200, openai_response('{"n":1}'))

    results = []
    for i in range(3):
        results.append(
            asyncio.run(
                acompletion(
                    target=LLMTarget(provider="openai", model="gpt-4o-mini"),
                    messages=[{"role": "user", "content": f"第 {i} 次"}],
                )
            )
        )

    eq(len(results), 3, "跨事件循环：三次调用的返回值都拿到了")
    check(
        all(r.text == '{"n":1}' for r in results),
        "跨事件循环：三次结果都正确",
        str([r.text for r in results]),
    )
    eq(len(_STATE["requests"]), 3, "跨事件循环：真的发了三次请求")


def main() -> int:
    server, base = _start_server()
    print(f"假厂商服务已启动：{base}\n")

    try:
        test_build_provider()
        test_target_from_settings()
        test_missing_key()
        test_openai_wire(base)
        test_openai_image_passthrough(base)
        test_google_wire(base)
        test_google_multi_part_text(base)
        test_google_http_image_degrades(base)
        test_no_text_when_blocked(base)
        test_errors(base)
        test_json_mode_shorthand(base)
        test_base_url_override(base)
        test_timeout(base)
        test_across_event_loops(base)
    finally:
        server.shutdown()
        server.server_close()

    print()
    if _FAILURES:
        print(f"❌ {len(_FAILURES)}/{_CHECKS} 条断言失败：")
        for item in _FAILURES:
            print(f"   - {item}")
        return 1
    print(f"✅ {_CHECKS} 条断言全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
