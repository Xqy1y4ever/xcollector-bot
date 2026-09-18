# xcollector-bot

Xcollector 的**消息处理层**。它独占 OneBot(NapCat) 连接，把 QQ 群里的官方通知
变成结构化条目：

> 接收消息 → 归一化 → 白名单筛选 → 原文入库 → 附件下载 → 抽取通知与截止时间
> → 建条目 → 计数 → 每日摘要推送 → 缺口检测 → 私聊指令

它是整套系统里唯一有「业务判断」的地方 —— 什么算通知、截止时间对不对、
今天有没有漏消息，都在这里决定。

> **项目主页与部署入口在
> [`xcollector-deploy`](https://github.com/Xqy1y4ever/xcollector-deploy)**
> —— 想看这套系统整体怎么跑、怎么装，从那里开始。

## 技术栈

| | |
|---|---|
| 语言 | Python 3.12+ |
| Web / WS | FastAPI + Uvicorn + websockets |
| 取数据 | OneBot v11 协议（NapCat 实现） |
| 大模型 | 自研网关，支持 OpenAI 格式与 Google 原生格式 |
| 依赖 | 只用到 `httpx`、`pydantic` —— 没有厂商 SDK |

## 架构

```
QQ 群 ──▶ NapCat ──OneBot WS──▶ xcollector-bot ──HTTP──▶ xcollector-backend
                                      ▲                        （SQLite）
                                      │
                                 前端「系统状态」页
```

| 属于 bot（本仓库） | 属于后端 |
|---|---|
| 连 OneBot / NapCat | 存储与表结构 |
| 群与发送者白名单 | 增删查改接口 |
| 抽取（规则 + 大模型 + 交叉验证） | 数据读投影 |
| 附件下载与上传 | 附件二进制存取 |
| 缺口检测、每日摘要、私聊指令 | 幂等与唯一性约束 |

**bot 不保存任何需要跨重启存活的状态**：待确认的 `/add`、`/list` 的编号映射、
「今天发过摘要没有」、没处理完的原文，全部存在后端。进程随时重启、升级都不会
丢失待办，也不会重复推送。

### 一次消息处理

```
接收 → 归一化 → 群白名单 ─┬─ 不在 → 结束
                          └─ 在   → 原文入库（POST /api/messages）
                                    → 附件下载并上传
                                    → 发送者白名单
                                    → 抽取（规则 / 大模型 / 两者）
                                    → 建通知条目（POST /api/notifications）
                                    → 计数
```

原文**先入库再处理**：QQ 消息不会重发，先落库保证「抽取途中进程被杀」不会导致
这条消息永久丢失。没处理完的消息标记为 `pending`，启动时、重连后、以及每 60 秒
会被自动捡回来继续处理。

## 部署

### Docker（推荐）

镜像由 CI 构建推送到 GHCR，编排在
[`xcollector-deploy`](https://github.com/Xqy1y4ever/xcollector-deploy) 仓库里。
bot 只发布到本机端口，NapCat 与前端通过它访问。

```bash
git clone https://github.com/Xqy1y4ever/xcollector-deploy.git
cd xcollector-deploy
cp .env.example .env     # 填两个令牌、群白名单、ONEBOT_*
sh preflight.sh
docker compose up -d
docker compose logs -f bot
```

bot 容器是**无状态**的，不需要挂卷。

### 本地直接跑

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

Copy-Item .env.example .env
# 至少确认 ONEBOT_MODE / ONEBOT_WS_URL / BACKEND_BASE_URL / API_TOKEN

.\.venv\Scripts\python.exe -m app.main
```

所有配置都有默认值，**没有任何配置也能启动**。没连上 NapCat 时
`/api/status` 会返回 `onebot.connected=false` 并带上错误原因。

## 连接 NapCat

NapCat 需要你自己安装并登录 QQ，两种接法二选一。

### 模式 A：bot 主动连 NapCat（默认，推荐）

`.env`：

```env
ONEBOT_MODE=client
ONEBOT_WS_URL=ws://127.0.0.1:3001
ONEBOT_ACCESS_TOKEN=
```

NapCat 里：**网络配置 → 新建 → WebSocket 服务器**，主机 `127.0.0.1`、
端口 `3001`、Token 与 `ONEBOT_ACCESS_TOKEN` 一致。

### 模式 B：NapCat 反向连 bot

`.env`：

```env
ONEBOT_MODE=server
ONEBOT_LISTEN_HOST=0.0.0.0
ONEBOT_LISTEN_PORT=8081
ONEBOT_LISTEN_PATH=/onebot/ws
```

NapCat 里：**网络配置 → 新建 → WebSocket 客户端**，
URL 填 `ws://<bot 所在机器 IP>:8081/onebot/ws`。

换模式后需要重启 bot。用 Docker 部署时，`ONEBOT_MODE=server` 还需要把 8081
端口发布出来（编排里默认已经发布）。

### 连不上，或者连上就被断开？

日志会直接说明原因。最常见的现象是「已连接」之后**立刻**断开：

```
INFO    OneBot 已连接 (client): ws://host.docker.internal:3001
WARNING OneBot 连接失败（1s 后重试）: ConnectionClosedOK: received 1005 ...
    ↳ 刚连上 0.1s 就被对端关闭（关闭码 1005）。最常见的原因是 NapCat 的 WebSocket
      服务器配了 Token，而 ONEBOT_ACCESS_TOKEN 没填或不一致 ...
```

按这个顺序查：

1. **Token 对不上** —— 头号原因。NapCat 的 WebSocket 服务器配置里如果填了
   Token，bot 这边的 `ONEBOT_ACCESS_TOKEN` 必须是**完全一样**的值。
   有些版本只认 URL 上的参数，那就把地址写成
   `ws://host.docker.internal:3001/?access_token=<token>`。
2. **看 NapCat 自己的日志** —— 关连接的是它，它才知道为什么。
3. **确认那个端口上确实是 NapCat 的 WebSocket 服务器**，不是它的 HTTP 服务、
   也不是另一个 OneBot 实现。
4. **别让两个东西连同一个 NapCat** —— 同一个 QQ 被两条链路收消息会重复入库。

连接断开后 bot 会自动重连：**稳定运行过一段时间再断开**（NapCat 重启、网络抖动）
是 1 秒后立刻重连；**连上就被踢**（多半是上面的配置问题）才会逐步退避到最多 60 秒。

## 群与发送者白名单

**两个都是白名单，留空 = 谁都不放行。** bot 只处理同时满足「群在
`GROUP_WHITELIST`」**且**「发送者在 `SENDER_WHITELIST`」的消息，其余的在
**写库之前**就被丢掉 —— 群里不进后端，库里也不留痕。

```env
# 格式都是 id:备注,id:备注（备注可省略）
GROUP_WHITELIST=123456789:官方通知群,987654321:教务处通知
SENDER_WHITELIST=10001:张老师,10002:李老师
# strict=只有名单里的发送者算（默认）
# off=这个群里谁发的都算（显式选择）
SENDER_WHITELIST_MODE=strict
```

三层过滤，越靠前越省：

| 判断 | 不在名单时 | 代价 |
|---|---|---|
| 群 | `group_filtered` | 一个后端请求都不发 |
| 发送者 | `skipped_whitelist` | 原文入库留档，但不抽取、不建条 |
| 噪声（闲聊） | `noise` | 原文入库，标记为噪声，不计入盲区 |

发送者不在名单时**原文仍然入库**是刻意的：万一名单配漏了，消息还在，之后
可以补处理；反过来直接丢弃就永久丢了。

白名单没配好时，启动日志会明确警告「会忽略所有消息」—— 否则表现出来只是
「bot 连上了但什么都不干」，很容易被当成连不上 NapCat 去查错方向。

## 大模型配置

模型拆成**提供商 + 模型名**两段配：提供商决定用哪种协议、默认打到哪个地址，
模型名原样发给对方。

```env
LLM_PRIMARY_PROVIDER=deepseek
LLM_PRIMARY_MODEL=deepseek-chat
```

内置的提供商见 `app/llm/providers.py`（deepseek / google / openai / openrouter /
groq / moonshot / zhipu / dashscope / siliconflow / ollama / vllm 等）。按提供商
配对应的 API key，例如 `deepseek` 配 `DEEPSEEK_API_KEY`、`google` 配
`GEMINI_API_KEY`。

两段分开的好处：模型名本身带斜杠也不会歧义 ——
`PROVIDER=openrouter` + `MODEL=anthropic/claude-sonnet-4`。

**想用哪个端点都可以**，填这三个就能接没内置的服务（自建、中转站、公司内网关）：

```env
LLM_PRIMARY_PROVIDER=myproxy
LLM_PRIMARY_API_BASE=https://llm.corp.example.com/v1
LLM_PRIMARY_API_KEY=xxx
# 可选：自定义端点走哪种协议，openai（默认）/ google
LLM_PRIMARY_API_KIND=
```

- **只想换个地址**（走代理、私有部署）而仍用内置提供商：只填 `LLM_PRIMARY_API_BASE` 即可。
- **交叉验证**：`LLM_SECONDARY_*` 四个字段与主模型一一对应，填了就启用。
  换个提供商打同一个模型名也算不同的模型，交叉验证照样有意义。

`EXTRACTOR=rule` 时完全不调用模型，只用规则抽取 —— 不需要 API key，
可以先这样把整条链路跑通再接入大模型。

配好之后建议先自检一次：

```powershell
.\.venv\Scripts\python.exe -m app.tools.check_llm
# 临时验证别的端点，不动配置
.\.venv\Scripts\python.exe -m app.tools.check_llm --provider google --model gemini-2.5-flash
```

## 私聊指令

只对 `COMMAND_WHITELIST` 里的 QQ 号生效，其他人发的私聊直接忽略。

| 指令 | 作用 |
|---|---|
| `/help` | 显示指令列表 |
| `/add <内容>` | 添加任务 |
| `/list [n]` | 查看待办，默认前 10 条 |
| `/done <编号>` | 标记完成 |
| `/del <编号>` | 移除 |
| `/cancel` | 取消待确认的添加 |

`/add` 解析不出明确截止时间时会先回显理解结果让你确认，回 `y` 才入库。
`/list` 显示的编号有 30 分钟有效期，过期后需要重新 `/list`。

## 接口

bot 对前端暴露这些接口，都带 `Authorization: Bearer <令牌>`：

| 方法 | 路径 | 用途 | 需要的权限 |
|---|---|---|---|
| GET | `/api/status` | 系统状态页数据 | 网页令牌即可 |
| GET | `/api/digest/preview` | 预览每日摘要 | 网页令牌即可 |
| POST | `/api/digest/send` | 立即推送摘要到 QQ | 管理令牌 |
| POST | `/api/send/private` | 发私聊消息 | 管理令牌 |
| POST | `/api/send/group` | 发群消息 | 管理令牌 |

**权限**：`WEB_API_TOKEN` 是给网页登录页用的，只能看状态和预览摘要；
`API_TOKEN`（或 `BOT_API_TOKEN`）是管理令牌，才能真的发消息。
范围不够返回 `403`，令牌不对返回 `401`。

发送失败返回 200 + `{"ok": false, "error": "..."}` 而不是 5xx ——
调用方要能区分「接口调不通」和「消息没发出去」。

## 配置

全部见 [`.env.example`](.env.example)，均带默认值。

| 配置 | 默认 | 说明 |
|---|---|---|
| `ONEBOT_MODE` | `client` | `client` = 主动连 NapCat；`server` = NapCat 反向连过来 |
| `ONEBOT_WS_URL` | `ws://127.0.0.1:3001` | client 模式下的 NapCat 地址 |
| `ONEBOT_ACCESS_TOKEN` | 空 | 与 NapCat 里配的 Token 一致 |
| `ONEBOT_LISTEN_PORT` | `8081` | server 模式的监听端口 |
| `BOT_LISTEN_HOST` / `BOT_LISTEN_PORT` | `127.0.0.1` / `8082` | bot 自己的 HTTP 接口 |
| `API_TOKEN` | 空 | **管理令牌**，同时也用于调用后端 |
| `WEB_API_TOKEN` | 空 | **网页令牌**，给前端登录页 |
| `BOT_API_TOKEN` | 空 | 想给 bot 单独一个管理令牌时填；留空用 `API_TOKEN` |
| `BACKEND_BASE_URL` | `http://127.0.0.1:8000` | 后端地址 |
| `GROUP_WHITELIST` | 空 | 要处理的群；**留空 = 一个群都不处理** |
| `SENDER_WHITELIST` | 空 | 要处理的发送者；**留空 = 一个发送者都不处理** |
| `SENDER_WHITELIST_MODE` | `strict` | `off` = 白名单群内不限发送者（显式放开） |
| `EXTRACTOR` | `llm` | `rule` / `llm` / `both` |
| `LLM_PRIMARY_PROVIDER` | `deepseek` | 主模型提供商（决定协议与默认地址） |
| `LLM_PRIMARY_MODEL` | `deepseek-chat` | 主模型名 |
| `LLM_PRIMARY_API_BASE` / `_API_KEY` | 空 | 覆盖提供商的地址 / 密钥（接自建端点用） |
| `LLM_PRIMARY_API_KIND` | 空 | 自定义端点的协议：`openai` / `google` |
| `LLM_SECONDARY_PROVIDER` / `_MODEL` | 空 | 交叉验证用的第二个模型；留空 = 关闭 |
| `LLM_SECONDARY_API_BASE` / `_API_KEY` / `_API_KIND` | 空 | 同上，作用于次模型 |
| `VLM_ENABLED` | `false` | 让模型一并识别图片里的截止时间 |
| `DIGEST_ENABLED` / `DIGEST_TIME` | `true` / `21:30` | 每日摘要开关与推送时刻 |
| `DIGEST_TARGET_QQ` | 空 | 摘要推送给谁（私聊） |
| `COMMAND_WHITELIST` | 空 | 允许发指令的 QQ 号；**留空 = 谁都不能发** |
| `GAP_ALERT_HOURS` | `2` | 群静默多久算缺口 |
| `LOW_CONFIDENCE_THRESHOLD` | `0.6` | 低于此置信度算「盲区」 |
| `MEDIA_DOWNLOAD_ENABLED` | `true` | 是否下载并保存附件 |
| `FORWARD_MAX_DEPTH` | `3` | 合并转发展开的深度上限 |
| `TZ` / `DIGEST_TZ` | `Asia/Shanghai` | 全系统统一时区 |

**注意两个端口别搞混**：`BOT_LISTEN_PORT`（默认 8082）是 bot 自己的 HTTP 接口，
一直监听；`ONEBOT_LISTEN_PORT`（默认 8081）只在 `ONEBOT_MODE=server` 时打开。

## 自检

不需要 NapCat 也能验证：

```powershell
# 离线（不联网）
.\.venv\Scripts\python.exe -m compileall -q app tests   # 语法
.\.venv\Scripts\python.exe -m tests.check_timeparse     # 中文相对时间解析
.\.venv\Scripts\python.exe -m tests.check_location      # 地点提取
.\.venv\Scripts\python.exe -m tests.check_commands      # 指令解析与排版
.\.venv\Scripts\python.exe -m tests.check_llm_gateway   # 大模型网关
.\.venv\Scripts\python.exe -m tests.check_onebot_reconnect  # 断线重连与提示

# 端到端（真 bot 代码 + 假后端，推荐先跑这个）
.\.venv\Scripts\python.exe -m tests.check_pipeline_e2e

# 模型配置（联网，只发 3 次请求）
.\.venv\Scripts\python.exe -m app.tools.check_llm
```

端到端测试覆盖：消息入库全流程与附件、白名单与噪声、缺口检测、崩溃恢复、
后端不可达时的降级、指令全链路、每日摘要、权限分级、HTTP 接口层。

## 注意事项

- **一个 QQ 号只跑一个 bot 实例**。同时跑两份会让同一条消息被推两遍。
- **附件是事后补齐的**。附件下载或上传失败时只保留原始地址，不影响通知本身。
- **`/done` 写入的状态值需要后端支持**。升级时先升后端，再升 bot。
- **全系统统一时区**（默认 `Asia/Shanghai`），否则摘要时间与列表时间会对不上。
