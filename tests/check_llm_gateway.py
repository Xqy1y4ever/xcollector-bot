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

from app.llm import LLMConfigError, LLMError, acompletion, resolve
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

def test_resolve() -> None:
    p, m = resolve("deepseek/deepseek-chat")
    eq(p.name, "deepseek", "resolve: 厂商前缀")
    eq(p.kind, "openai", "resolve: deepseek 走 openai 协议")
    eq(m, "deepseek-chat", "resolve: 模型名")

    p, m = resolve("google/gemini-2.5-flash")
    eq(p.kind, "google", "resolve: google 走 google 协议")
    eq(m, "gemini-2.5-flash", "resolve: google 模型名")

    p, m = resolve("gemini/gemini-2.5-pro")
    eq(p.name, "google", "resolve: gemini 是 google 的别名")
    eq(m, "gemini-2.5-pro", "resolve: gemini 别名模型名")

    # 模型名本身带斜杠：只有第一段是厂商
    p, m = resolve("openrouter/anthropic/claude-sonnet-4")
    eq(p.name, "openrouter", "resolve: 多段斜杠取第一段当厂商")
    eq(m, "anthropic/claude-sonnet-4", "resolve: 多段斜杠其余全是模型名")

    # 无前缀 → openai
    p, m = resolve("gpt-4o-mini")
    eq(p.name, "openai", "resolve: 无前缀按 openai")
    eq(m, "gpt-4o-mini", "resolve: 无前缀模型名")

    # 空模型名要报配置错误
    for bad in ("", "   ", "deepseek/"):
        try:
            resolve(bad)
            check(False, f"resolve: {bad!r} 应当报错")
        except LLMConfigError:
            check(True, f"resolve: {bad!r} 报 LLMConfigError")

    # 未知前缀且没配 base → 明确报错，并提示该设哪个变量
    os.environ.pop("MYSTERY_API_BASE", None)
    try:
        resolve("mystery/some-model")
        check(False, "resolve: 未知厂商应当报错")
    except LLMConfigError as exc:
        check("MYSTERY_API_BASE" in str(exc), "resolve: 未知厂商报错里给出要设的环境变量", str(exc))

    # 未知前缀但配了 base → 当作自定义 OpenAI 兼容端点
    os.environ["MYSTERY_API_BASE"] = "http://127.0.0.1:1/v1"
    p, m = resolve("mystery/some-model")
    eq(p.kind, "openai", "resolve: 自建端点按 openai 协议")
    eq(p.resolved_base_url(), "http://127.0.0.1:1/v1", "resolve: 自建端点 base 来自环境变量")
    check(not p.requires_key, "resolve: 自建端点不强制要 key")
    os.environ.pop("MYSTERY_API_BASE", None)


def test_missing_key() -> None:
    for env in PROVIDERS["openai"].key_envs:
        os.environ.pop(env, None)
    try:
        asyncio.run(
            acompletion(model="openai/gpt-4o-mini", messages=[{"role": "user", "content": "hi"}])
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
            model="openai/deepseek-chat",
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
            model="openai/gpt-4o-mini",
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
            model="google/gemini-2.5-flash",
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
        acompletion(model="google/gemini-2.5-flash", messages=[{"role": "user", "content": "x"}])
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
                model="google/gemini-2.5-flash",
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
            acompletion(model="google/gemini-2.5-flash", messages=[{"role": "user", "content": "x"}])
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
                acompletion(model="openai/gpt-4o-mini", messages=[{"role": "user", "content": "x"}])
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
            acompletion(model="openai/gpt-4o-mini", messages=[{"role": "user", "content": "x"}])
        )
        check(False, "200 + body.error 应当报错")
    except LLMError as exc:
        check("quota exceeded" in str(exc), "200 + body.error 也要抛错", str(exc))

    # 200 但没有 choices
    _STATE["responder"] = lambda path: (200, {"usage": {}})
    try:
        asyncio.run(
            acompletion(model="openai/gpt-4o-mini", messages=[{"role": "user", "content": "x"}])
        )
        check(False, "缺 choices 应当报错")
    except LLMError as exc:
        check("choices" in str(exc), "缺 choices 的报错可读", str(exc))

    # 200 但返回 HTML（反代打错了很常见）
    _STATE["responder"] = lambda path: (200, "<html>502 Bad Gateway</html>")
    try:
        asyncio.run(
            acompletion(model="openai/gpt-4o-mini", messages=[{"role": "user", "content": "x"}])
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
                model="openai/gpt-4o-mini",
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
            model="openai/gpt-4o-mini",
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
            model="openai/gpt-4o-mini",
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
        acompletion(model="deepseek/deepseek-chat", messages=[{"role": "user", "content": "x"}])
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
                    model="openai/gpt-4o-mini",
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
        test_resolve()
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
