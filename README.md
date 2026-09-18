# xcollector-bot

Xcollector 的**消息处理层**：独占 OneBot(NapCat) 连接，把 QQ 群里的官方通知
变成结构化条目 —— 接收消息 → 归一化 → 白名单筛选 → 原文入库 → 附件下载 →
按订阅决定给谁抽 → 抽取通知与截止时间 → 扇出给每个订阅者 → 计数 → 每日摘要 →
缺口检测 → 私聊指令（含 `/注册`、`/订阅`）。

它是整套系统里唯一做「业务判断」的地方：什么算通知、截止时间对不对、
今天有没有漏消息，都在这里决定。

- 技术栈：Python 3.12+ · FastAPI · websockets · OneBot v11（NapCat 实现）
- 多用户：处理**所有用户订阅的并集**，一条消息只抽一次，再扇给每个订阅者；
  没人订阅的来源不抽取（LLM 调用是唯一花钱的地方）
- 不保存跨重启状态：待确认的 `/add`、`/list` 编号、今天发没发过摘要，
  全部存在后端

> 整套系统怎么部署见
> [`xcollector-deploy`](https://github.com/Xqy1y4ever/xcollector-deploy) 的 README。
> 本文只讲这个仓库自己怎么跑起来。

## 部署

### 前置

1. 一个**已登录的 NapCat**（见下「接 NapCat」）；
2. 一个跑起来的 [`xcollector-backend`](https://github.com/Xqy1y4ever/xcollector-backend)；
3. 一个**服务令牌** `API_TOKEN`，与后端 `.env` 里那个**同一个值**：
   `python -c "import secrets;print(secrets.token_hex(32))"`。

### Docker（推荐）

```bash
cd xcollector-deploy/bot
cp .env.example .env
# 至少改：API_TOKEN（与 backend 那边同一个值）、GROUP_WHITELIST、SENDER_WHITELIST、ONEBOT_WS_URL
./preflight.sh
./start.sh
```

镜像名：`ghcr.io/xqy1y4ever/xcollector-bot:latest`。bot 容器**没有卷**，
它不保存任何要跨重启存活的状态，容器随便删。

> 前提是 backend 那个栈已经起过 —— bot 靠它建的 Docker 网络（默认 `xcollector`）
> 用服务名 `backend` 找到后端。先起 bot 会直接报「找不到网络」并告诉你该去哪个目录。

### 本地跑

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows；Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8082
```

### 接 NapCat

NapCat 需要开一个 WebSocket 服务（NapCat 面板里配），bot 用 `client` 模式连过去：

```env
ONEBOT_MODE=client
ONEBOT_WS_URL=ws://127.0.0.1:3001
ONEBOT_ACCESS_TOKEN=          # 与 NapCat 里配的 access token 一致；留空=不校验
```

反向模式（NapCat 连 bot）把 `ONEBOT_MODE=server`，并配
`ONEBOT_LISTEN_HOST/PORT/PATH`。容器里连宿主机上的 NapCat 用
`ws://host.docker.internal:3001`（deploy 里的 `start.sh` 已经加了
`--add-host host.docker.internal:host-gateway`，Linux 上也能用这个名字）。

连不上不会让 bot 崩，只是收不到消息；状态见 `GET /api/status` 或网页的「系统状态」页。

### 配置

完整说明见 [`.env.example`](.env.example)（每一项都写了为什么）。要点：

| 变量 | 默认 | 说明 |
|---|---|---|
| `ONEBOT_MODE` / `ONEBOT_WS_URL` / `ONEBOT_ACCESS_TOKEN` | `client` / `ws://127.0.0.1:3001` / 空 | OneBot 连接方式与凭据 |
| `API_TOKEN` | 空 | **服务令牌**，必须与后端一致；调后端与 bot 自己的 `/api/*` 都用它 |
| `BOT_API_TOKEN` | 空 | 想让 bot 的管理接口用另一个令牌时填，留空则用 `API_TOKEN` |
| `BACKEND_BASE_URL` | `http://127.0.0.1:8000` | 后端地址 |
| `GROUP_WHITELIST` | 空 | 要处理的群，`群号:名称,群号:名称`。**留空 = 一个消息都不处理** |
| `SENDER_WHITELIST` | 空 | 要处理的发送者，`QQ号:名称`。**留空 = 一个消息都不处理** |
| `SENDER_WHITELIST_MODE` | `strict` | `strict` = 只有名单里的发送者算；`off` = 该群里谁发的都算（**显式**放开） |
| `EXTRACTOR` | `llm` | `rule` 只跑规则（不花钱）/ `llm` / `both`（模型 + 规则交叉判断） |
| `LLM_PRIMARY_PROVIDER` / `LLM_PRIMARY_MODEL` | `deepseek` / `deepseek-chat` | 主模型。自建/中转端点填 `LLM_PRIMARY_API_BASE` + `_API_KEY`（提供商名随便起） |
| `LLM_SECONDARY_*` | 空 | 交叉验证用的第二个模型，四项都留空 = 不做交叉验证 |
| `VLM_ENABLED` | `false` | 把图片也喂给模型（官方通知常把 DDL 写在图里） |
| `MEDIA_DOWNLOAD_ENABLED` / `MEDIA_MAX_BYTES` | `true` / `5242880` | 附件由 bot 下载字节再上传给后端 |
| `DIGEST_ENABLED` / `DIGEST_TIME` / `DIGEST_TZ` | `true` / `21:30` / `Asia/Shanghai` | 每日摘要。`DIGEST_TARGET_QQ` 留空 = **每个注册用户各收自己那份**；填了 = 只发给这一个 QQ |
| `GAP_ALERT_HOURS` | `2` | 群静默多久算缺口（会产生一条告警） |
| `LOW_CONFIDENCE_THRESHOLD` | `0.6` | 低于这个置信度的截止时间算「盲区」 |
| `COMMAND_PREFIX` | `/` | 指令前缀 |
| `COMMAND_WHITELIST` | 空 | 允许发指令的 QQ，`QQ号:备注`。**留空 = 除 `/注册` 与 `/help` 外谁都不能发指令**（这两个是注册入口，不受限制） |
| `LOG_LEVEL` | `INFO` | 日志级别 |

LLM 的 key 按「实际用到的厂商」配一个即可（`DEEPSEEK_API_KEY` / `OPENAI_API_KEY` /
`GEMINI_API_KEY` …），清单见 `.env.example` 末尾。

### 确认在跑

```bash
# 模型配置能不能用（会真发一次请求）
python -m app.tools.check_llm

# 不装 NapCat、不花模型的钱，喂一条假事件走完整流水线（需要后端；或先起 fake_backend）
python -m app.tools.fake_backend --port 9000 &
python -m app.tools.feed_event --backend-url http://127.0.0.1:9000 --extractor rule --count 1
```

容器自带 HEALTHCHECK（探 `/api/status`）；`GET /api/status` 需要管理令牌。

## 常用指令

在 QQ 私聊里发给机器人（`COMMAND_WHITELIST` 里的人才能用；`/help`、`/注册`
不受此限制）：

| 指令 | 作用 |
|---|---|
| `/help` | 显示可用指令 |
| `/注册` | 拿注册验证码（到网页上用「QQ 号 + 验证码 + 邀请码」完成注册） |
| `/add <内容>` | 添加任务，例：`/add 明天下午3点 交实验报告` |
| `/list [n]` | 查看待办（默认前 10 条） |
| `/done <编号>` / `/del <编号>` / `/cancel` | 标记完成 / 移除 / 取消待确认的添加 |
| `/来源 [关键词]` | 看有哪些可以订的来源 |
| `/订阅 <群号> <发送者QQ> [备注]` 或 `/订阅 <编号>` | 订某个群里某个人发的通知 |
| `/订阅列表` / `/退订 <编号>` | 管理自己的订阅 |

## 许可

MIT
