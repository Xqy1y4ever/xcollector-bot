# xcollector-bot

Xcollector 的**消息处理层**：独占 OneBot(NapCat) 连接，负责

> 归一化 → 群/发送者白名单 → **写前日志** → 附件上下载 → 抽取（规则 + LLM）→
> 建通知 → 统计 → 每日 digest → 缺口检测 → 私聊指令

后端 `xcollector-backend` 退化成**纯 CRUD 数据层**：只做增删查改、读投影、附件存取，
不做任何业务判断。三边唯一的约定是 `xcollector-backend/docs/api.md`。

---

## 0. 职责边界（改代码前先读）

| 属于 **bot**（本仓库） | 属于 **backend** |
|---|---|
| 连 OneBot / NapCat | 存储（SQLite）与表结构 |
| 群 / 发送者白名单筛选 | 增删查改接口 |
| 抽取（规则 + LLM + 交叉验证） | **读投影**（correction 覆盖 notification、由 `due_at` 推导 status） |
| 附件下载字节并上传 | 附件二进制存取 |
| 合并转发展开、每条消息一行日志 | 唯一性约束与幂等 |
| 缺口检测、digest 组装与发送、统计计数 | 存储健康检查 |
| 私聊指令 | **不认识** QQ / OneBot / 白名单 / LLM / digest 这些词 |

**bot 不允许持有任何需要跨重启存活的状态**。判据很简单：进程被 `kill -9` 再起来，
哪些东西丢了会让用户困惑或被打扰？

| 状态 | 放哪 | 为什么 |
|---|---|---|
| `/add` 的待确认（等用户回 y/n） | 后端 `bot_state`（`command_pending`，TTL=PENDING_TTL_SECONDS） | 用户回 `y` 时 bot 刚重启过，那条待确认不该凭空消失 |
| `/list` 的「编号 → 通知 id」映射 | 后端 `bot_state`（`command_list`，TTL=30 分钟） | 编号是给人看的短期状态；过期后 `/done 3` 会要求先刷新列表，**绝不猜** |
| digest「今天发过没有」 | 后端 `digest_log` | 重发对收件人是骚扰，比漏发更糟 |
| 未处理完的原文 | 后端 `raw_message.state` | `state=pending` 就是崩溃恢复队列 |
| OneBot 连接对象、群名缓存 | bot 内存 | 纯缓存：丢了会重新查，后端 `group_state` 里也有 |
| 「连原文都没写进后端」的消息 | bot 内存 + ERROR 日志 | 罕见故障；计数暴露在 `/api/status` 的 `pipeline.pending_retry` 上 |

---

## 1. 数据流

```
QQ/NapCat ──OneBot WS──▶ xcollector-bot
                              │
                              ├─ 归一化（消息段 / 合并转发 / 群名）
                              ├─ 群白名单（纯本地，不在就 DEBUG 日志结束）
                              ├─ 【写前日志】POST /api/messages      ← 原文先落库
                              ├─ 附件：下载字节 → POST /api/attachments → PATCH /api/messages/{id}
                              ├─ POST /api/groups                    → previous_last_msg_ts
                              ├─ 发送者白名单（不在 → PATCH state=skipped_whitelist）
                              ├─ 抽取（rule / llm / both）
                              ├─ POST /api/notifications（或 PATCH state=noise/unparsed/degraded）
                              └─ POST /api/stats

                        前端 ──▶ bot /api/status（系统状态页）
                        前端 ──▶ backend /api/notifications（通知台）
```

### 为什么原文必须先落库（写前日志）

QQ 群消息是**唯一不可再生的资产**（QQ 不会重发）。附件下载、LLM 抽取都可能慢、
可能崩、可能超时；如果把 `POST /api/messages` 放在它们后面，"抽取途中进程被杀"
就等于"这条消息永远不存在"。所以顺序是：**先写原文，再做一切重活**，
附件是事后 `PATCH attachments` 补齐的（契约第 1 节专门为此放开了这个字段）。

配套的**崩溃恢复**：`state=pending` 既是状态标记，也是恢复队列。
`pipeline/runner.resume_pending()` 会在 **启动时、OneBot 重连成功后、以及每 60 秒**
把还停在 pending 的消息捡回来继续处理。没有任何一条消息会永远停在那里没人管。

**已删除**：改造前的 200ms 攒批缓冲。每条消息现在都要下载附件、调 LLM、写好几次
后端，根本没法批处理；那个缓冲只会变成一个"进程被 kill 时缓冲区里的原始消息直接
消失、事后还不知道丢了几条"的窗口。

---

## 2. 快速开始

```powershell
cd xcollector-bot

# 1) 虚拟环境（Python 3.12+；本机用 3.14 验证过）
python -m venv .venv

# 2) 依赖（一次装完，LLM 抽取没有额外依赖）
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 3) 配置
Copy-Item .env.example .env
#    至少确认 ONEBOT_MODE / ONEBOT_WS_URL / BACKEND_BASE_URL / API_TOKEN

# 4) 启动
.\.venv\Scripts\python.exe -m app.main
```

**没有任何配置也能启动**：所有项都有默认值。没连上 NapCat 时 `/api/status` 会返回
`onebot.connected=false` 并带上 `last_error` —— 这本身就是一个可用的自检结果。

> `EXTRACTOR=rule` 时整条链路完全不碰模型：核心是"收到消息 → 入库 →
> 建通知 → digest"，模型只是提高召回率。**LLM 抽取不需要额外装任何东西** ——
> 多厂商调用走 `app/llm/` 里自己写的网关，只用到本来就有的 `httpx`。

### 换模型 / 接自己的网关（`app/llm/`）

模型名写成 `厂商/模型名`，网关自己认前缀并选协议，**不需要改代码、也不需要装 SDK**：

```bash
LLM_PRIMARY_MODEL=deepseek/deepseek-chat        # DeepSeek（OpenAI 格式）
LLM_PRIMARY_MODEL=google/gemini-2.5-flash       # Gemini（Google 原生 generateContent）
LLM_PRIMARY_MODEL=openrouter/anthropic/claude-sonnet-4
LLM_PRIMARY_MODEL=myproxy/qwen3-32b             # 自建/公司内网关
```

- 内置厂商和对应的 key 环境变量见 `app/llm/providers.py`（也列在 `.env.example` 里）。
- **base URL 可覆盖**：`{厂商名大写}_API_BASE`，例如 `DEEPSEEK_API_BASE=https://proxy.example.com/v1`
  —— 走代理、走中转站、换私有部署都靠它。
- **没注册过的前缀也能用**：只要配上 `{前缀大写}_API_BASE`，就按 OpenAI 兼容端点处理
  （加上 `{前缀大写}_API_KEY` 如果它要鉴权）。所以接任何自建服务都不用改这个仓库。
- `LLM_SECONDARY_MODEL` 填另一个厂商的模型就能启用交叉验证，两个模型对 `due_at`
  不一致时会把置信度压到 0.5 并标记 `conflict`（宁可疑，不装懂）。

`app/llm/` 是**自己写的**（约 400 行，只依赖本来就有的 `httpx`），没有用 litellm：
我们只需要「用统一格式调几个厂商的 chat 接口」这一件事，而 litellm 会拖进
openai / tokenizers / tiktoken / aiohttp / jsonschema 一整棵依赖树，镜像多出上百 MB。

网关刻意**只做单次尝试、不做重试** —— 重试策略（第一次用 JSON 模式、失败退回普通模式）
在 `pipeline/extract.py::run_llm` 里，那里才知道业务规则；网关层再重试只会和它打架、
让超时失控。失败时抛的 `LLMError` 带 `status`，上层据此判断值不值得重试。

> 图片：OpenAI 侧原样传 `image_url` + data URL；Google 侧转成 `inlineData`。
> Google 原生接口读不了任意 http 图片地址，这时**降级成一句占位文本并打 WARNING**
> —— 宁可少一张图，也不能让整条消息抽取不出来；但也绝不静默丢，否则就是悄悄漏 DDL。

### 两个端口，别搞混（最常见的配置事故）

| 环境变量 | 默认 | 谁连谁 | 什么时候用 |
|---|---|---|---|
| `ONEBOT_LISTEN_PORT` | `8081` | **NapCat → bot**（反向 WS） | 只在 `ONEBOT_MODE=server` 时监听 |
| `BOT_LISTEN_PORT` | `8082` | **前端 / 后端 → bot**（HTTP） | 一直监听 |

`ONEBOT_MODE=client` 时 8081 **根本不会打开**（此时 bot 主动去连 NapCat 的 3001）。

---

## 3. NapCat 两种模式

### 模式 A：`ONEBOT_MODE=client`（推荐，默认）

```env
ONEBOT_MODE=client
ONEBOT_WS_URL=ws://127.0.0.1:3001
ONEBOT_ACCESS_TOKEN=
```

NapCat：**网络配置 → 新建 → WebSocket 服务器**，主机 `127.0.0.1`、端口 `3001`、Token 与
`ONEBOT_ACCESS_TOKEN` 一致。

### 模式 B：`ONEBOT_MODE=server`（反向 WS）

```env
ONEBOT_MODE=server
ONEBOT_LISTEN_HOST=0.0.0.0
ONEBOT_LISTEN_PORT=8081
ONEBOT_LISTEN_PATH=/onebot/ws
```

NapCat：**网络配置 → 新建 → WebSocket 客户端**，URL `ws://<bot所在机器IP>:8081/onebot/ws`。

两种模式的下游代码完全一样（`onebot/hub.py` 里的 `_Conn` 把 websockets 和 Starlette 的
WebSocket 归一化），随时可切，不用改业务代码。换模式后要重启。

---

## 4. 指令

只对 `COMMAND_WHITELIST` 里的 QQ 号生效，其他人发的私聊消息**直接忽略**（只记 DEBUG）。

| 指令 | 作用 |
|---|---|
| `/help` | 显示指令列表 |
| `/add <内容>` | 添加任务（**在 bot 本地解析**：规则 + LLM） |
| `/list [n]` | 查看待办，默认前 10 条 |
| `/done <编号>` | 标记完成（`corrections` 写 `status=done`） |
| `/del <编号>` | 移除（写 `status=archived`） |
| `/cancel` | 取消待确认的添加 |

### `/add`

- **有把握** → `POST /api/messages` + `POST /api/notifications`，回执：

  ```
  ✅ 已添加：交实验报告
  🕒 截止：09-17 15:00（明天下午3点）
  📍 地点：教三201
  🔖 编号：#3f9a2c
  ```

- **没把握**（解析不出截止时间 / 置信度低于 `LOW_CONFIDENCE_THRESHOLD` / 模型不可用）→
  **不建条**，把理解结果回显给用户确认，并把待确认状态写进后端 `bot_state`：

  ```
  ⚠️ 我没解析出明确的截止时间。
  我理解为：尽快把材料交上来
  截止：未识别
  仍然添加吗？回复 y 确认，n 取消。
  ```

  回 `y` / `yes` / `是` / `确认` → 用**用户当初看到的那次解析结果**建条；回 `n` → 丢弃。
  bot 在中途重启也不影响这次确认。

手动任务入库的形状和群消息完全一样（`group_id = manual:<QQ号>`、`group_name = 手动添加`、
`message_id = manual-<毫秒>`），所以前端只需要认识一种数据。

### 编号映射会过期（这是刻意的）

`/done 3` 里的"3"是**给人看的短期状态**（TTL 30 分钟）。过期或不存在时，
bot 回「编号列表已过期或还没生成，请先发 /list 刷新列表」，**绝不猜** ——
猜错的代价是把用户没打算动的那条通知标成完成。

### 后端不可用时

指令回「后端暂时不可用，请稍后再试」，**不做退避重试**（用户在线等，让他干等 7 秒
比直接说"稍后再试"更糟）。消息路径才用 1s/2s/4s 的退避。

---

## 5. 接口

### 5.1 bot 暴露给前端 / 后端

所有请求带 `Authorization: Bearer <BOT_API_TOKEN>`。为空时**回退用 `API_TOKEN`**，
两个都为空才跳过校验（仅本地开发，启动时打 WARNING）。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/status` | 系统状态页（契约第 9 节的完整结构） |
| GET | `/api/digest/preview` | `{"text": "..."}` |
| POST | `/api/digest/send` | `{"dry_run": true}` → `{"ok","sent","text","error"}` |
| POST | `/api/send/private` | 发私聊（`{"user_id","message"}`） |
| POST | `/api/send/group` | 发群消息 |

**发送失败不返回 5xx**，而是 200 + `{"ok": false, "error": "OneBot 未连接：..."}` ——
调用方要能区分"调用失败"（4xx/5xx）和"发送失败"（ok=false）。

**后端不可达时 `/api/status` 仍然返回 200**：每个后端调用都有兜底默认值，
`backend.reachable=false` + `backend.error` 就是答案本身。状态页要能显示"后端挂了"。

`/api/status` 里所有计数都是**现算**的：

| 字段 | 怎么算 |
|---|---|
| `pipeline.today_*` | `GET /api/stats?day=<今天>` |
| `blindspots.unparsed_count` | `GET /api/messages?state=unparsed&state=degraded&since=<7天前>&count_only=1` |
| `blindspots.conflict_count` / `low_confidence_count` | `GET /api/notifications`（**契约没有暴露这两个过滤参数，只能在 bot 侧数**） |
| `groups` | `GET /api/groups`（真相来源）+ 内存里的群名缓存 |
| `gap_alerts` | `GET /api/gap-alerts?acknowledged=false` |
| `digest.sent_today` | `GET /api/digest-log?day=<今天>&kind=auto&sent=true&count_only=1` |

### 5.2 bot → 后端

基础地址 `BACKEND_BASE_URL`，认证头 `Authorization: Bearer <API_TOKEN>`
（整套系统只有一个共享密钥，两边都叫 `API_TOKEN`）。契约见
`xcollector-backend/docs/api.md`。bot 用到的接口：

`POST/GET/PATCH /api/messages`、`POST/GET/PATCH/DELETE /api/notifications`、
`POST /api/notifications/{id}/corrections`、`POST /api/attachments`、
`POST/GET /api/groups`、`POST/GET /api/gap-alerts`、`POST/GET /api/stats`、
`POST/GET /api/digest-log`、`PUT/GET/DELETE /api/state/{ns}/{key}`、`GET /api/health`。

**重试策略**（`backend_client.py` 一处实现）：

| 场景 | 重试 | 说明 |
|---|---|---|
| 写接口（messages / notifications / groups / stats / gap-alerts / digest-log / state） | `BACKEND_MAX_RETRIES` 次，退避 1s/2s/4s | 契约保证所有写接口幂等，重试是安全的 |
| 读接口、指令路径 | 0 次 | 用户和状态页都在等，失败要快 |
| 附件上传 | `BACKEND_MAX_RETRIES` 次 | 失败即降级为"只存 source_url"，绝不因为一张图丢通知 |

4xx 里只有 `404/405/408/425/429` 会被当成"等一会儿可能就好了"（后端重启时路由
还没挂上），其余 4xx 立刻失败。重试仍失败的处理见第 0 节那张表。

---

## 6. 自检（没有 NapCat 也能验证）

### 6.1 离线检查

```powershell
.\.venv\Scripts\python.exe -m compileall -q app tests   # 语法
.\.venv\Scripts\python.exe -m tests.check_timeparse     # 中文相对时间，18 条
.\.venv\Scripts\python.exe -m tests.check_location      # 规则抽取地点，14 条
.\.venv\Scripts\python.exe -m tests.check_commands      # 指令解析与排版（纯函数）
.\.venv\Scripts\python.exe -m tests.check_llm_gateway   # LLM 网关的线上格式，96 条
```

`check_llm_gateway` 会起一个**真的本地 HTTP 服务**当假厂商（不是 mock transport，
否则测不出 URL 和 header 拼错），把两种协议的请求和响应都验一遍，并且**完全不联网**。

### 6.1b 模型配置自检（要联网，只发 3 次请求）

```powershell
.\.venv\Scripts\python.exe -m app.tools.check_llm
.\.venv\Scripts\python.exe -m app.tools.check_llm --model google/gemini-2.5-flash
```

逐层验：模型名解析 → key 有没有配 → 连通性 → JSON 模式 → **真实抽取链路**
（拿两条样例通知跑完整的 `extract_with_llm`）。任何一层不通就非 0 退出。

这条命令存在的理由就是这条工作流最大的风险不是「模型答错」，而是
**「模型静默不可用」**：key 过期、余额耗尽、模型名写错、反代把 base URL
改坏了，你却不知道，直到某天发现漏了一堆通知。配好之后先跑一遍。

### 6.2 端到端（真 bot 代码 + 假后端，推荐先跑这个）

```powershell
.\.venv\Scripts\python.exe -m tests.check_pipeline_e2e
```

它在本进程内起一个真 uvicorn 的 `app.tools.fake_backend`，然后：

1. 喂一条带图片的 OneBot 群事件，断言**后端调用序列**与入库数据；
2. 白名单（群 / 发送者）、噪声、缺口检测；
3. **崩溃恢复**：塞一条 `state=pending`，验证被捡回来处理完；
4. **后端不可达**：`/api/status` 不炸、消息处理不崩、指令回「后端暂时不可用」；
5. 指令全链路：`/add`（直接建 + 回问确认）、`/list`、`/done`、`/del`，含"bot 重启后仍有效"；
6. digest 预览 / 发送 / "今天发过没有"（换一个实例依然不重发）；
7. 共享密钥：带对 / 带错令牌；
8. `GET /api/status`、`/api/digest/preview`、`POST /api/digest/send` 的 HTTP 层。

### 6.3 手工跑假后端

```powershell
# 终端 A：假后端（前 2 次 POST /api/messages 故意 500，用来看退避重试）
.\.venv\Scripts\python.exe -m app.tools.fake_backend --port 9000 --fail-first 2

# 终端 B：跑完整条流水线（真代码，只把 hub 换成 FakeHub）
.\.venv\Scripts\python.exe -m app.tools.send_test --backend-url http://127.0.0.1:9000
.\.venv\Scripts\python.exe -m app.tools.feed_event --backend-url http://127.0.0.1:9000 --count 3

# 看假后端到底收到了什么、按什么顺序
Invoke-RestMethod http://127.0.0.1:9000/api/_fake/state | ConvertTo-Json -Depth 6
```

### 6.4 真启动

```powershell
$env:ONEBOT_WS_URL='ws://127.0.0.1:3999'    # 指向死端口：不碰真实 NapCat
$env:BACKEND_BASE_URL='http://127.0.0.1:9000'
.\.venv\Scripts\python.exe -m app.main
Invoke-RestMethod http://127.0.0.1:8082/api/status | ConvertTo-Json -Depth 6
```

---

## 7. 已知边界

1. **bot 只有一处内存状态需要留意**：`PendingWrites`（"连原文都没写进后端"的消息）。
   它不落盘，靠 `待重试=是` 的 ERROR 日志 + `/api/status` 的 `pipeline.pending_retry`
   计数暴露出来。这是罕见故障（后端 4xx / 一直不可达），属于"如实上报"而不是"静默丢"。
   已经落库、只是没处理完的消息不在这里 —— 它们在 `state=pending` 上，重启也能恢复。
2. **附件是"事后补齐"**。下载/上传失败时附件降级为只存 `source_url`（原 QQ CDN 地址），
   不影响通知本身；附件字节放在后端，前端走 `/api` 代理直接看。
3. **`file` 类型的附件可能拿不到 URL**。NapCat 对文件段有时只给本地路径，
   那个路径在 bot 机器之外没有意义，所以只留下 `name`。
4. **合并转发有深度上限**（`FORWARD_MAX_DEPTH=3`）。超深时留
   `[合并转发层级超过上限，已省略]` 占位符：内容不全，但不丢消息。
5. **群名查询有 5 分钟负缓存**。群号配错或机器人已退群时 `group_name` 为 `null`，
   这是刻意的，否则每条消息都要等一次必然失败的 API 调用。群名只是缓存，
   真相来源是后端 `group_state`。
6. **`blindspots` 里的 conflict / 低置信度计数是 bot 侧数的**。
   契约的 `GET /api/notifications` 只暴露 `status` / `q` / `since` / `limit`，
   没有 `conflict` 和 `due_confidence` 过滤参数（传了也会被静默忽略，
   那样会数出"全部通知"这种静默错误）。所以这两个数字是把通知拉回来自己数的。
7. **`/done` 会写 `status=done`**。后端若只认 `active/archived`，用户会收到
   「操作被后端拒绝」。升级顺序：先后端，后 bot。
8. **时间渲染固定 UTC+8**（`DIGEST_TZ` 默认 `Asia/Shanghai`）。全系统一个时区，
   才不会出现"digest 说 21:30、列表说 13:30"。
9. **单进程单连接**。同时跑两份 bot 会让 NapCat 把同一条消息推两遍
   （入库幂等，但会白白抽两次）。一个 QQ 号只应该有一个 bot 实例。
