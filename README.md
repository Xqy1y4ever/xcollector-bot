# xcollector-bot

Xcollector 的**机器人进程**：独占 OneBot(NapCat) 连接，把 QQ 消息规范化后推给 backend，
并作为机器人接收私聊指令（比如添加任务）。

这个仓库是从 `xcollector-backend` 里**拆出来**的。拆之前，OneBot 连接和抽取/存储挤在同一个进程里，
后果是：连个 WS 的进程同时背着 LLM 调用和 SQLite 锁，重启任何一边都要一起重启；
NapCat 掉线会连带抽取停摆。现在两者的生命周期完全独立。

---

## 0. 配置边界（谁配谁）

配置按**进程**划分，不按功能划分。判据是：这个值改变时，需要重启哪个进程？

| 配置 | 属于 | 为什么 |
|---|---|---|
| `ONEBOT_*`、`FORWARD_MAX_DEPTH` | **bot** | 只跟 QQ 接入有关。后端不认识 OneBot 协议，也不需要知道合并转发展开深度 |
| `COMMAND_*`、`PENDING_TTL_SECONDS` | **bot** | 指令是 bot 的能力 |
| `BOT_LISTEN_HOST/PORT`、`BOT_API_TOKEN` | **bot** | 它自己的 HTTP 入口 + 校验别人递来的令牌 |
| `GROUP_WHITELIST`、`SENDER_WHITELIST` | **backend** | 收什么、抽什么是**策略**，策略只有一份，放在后端（bot 照单全推，后端过滤） |
| `EXTRACTOR`、`LLM_*`、`VLM_*` | **backend** | 抽取逻辑在后端 |
| `DB_PATH`、`ATTACHMENT_DIR`、`MEDIA_*` | **backend** | 数据落在后端这一侧。附件由后端下载 —— 否则前后端分开部署时前端取不到图 |
| `DIGEST_*` | **backend** | 后端决定摘要内容，然后调 bot 去发 |
| `BACKEND_BASE_URL`、`BACKEND_TIMEOUT/RETRIES` | **bot** | bot 要知道后端在哪、等多久、重试几次 |
| `INGEST_API_TOKEN` | **两边，同一个名字** | bot 递出、后端校验。**同一个密钥在整套系统里只有一个名字**，不需要猜「这两个名字其实是一回事」 |
| `LOG_LEVEL`、`LOG_PREVIEW_CHARS` | 两边各自 | 每个进程有自己的日志，名字相同但互不影响 |

> **一个刻意的取舍**：群/发送者白名单只在后端。代价是 bot 会把它所在的所有群都推过来，
> 后端再过滤掉不要的。好处是策略只有一处定义 —— 如果 bot 也存一份白名单，
> 改配置就要改两个文件，那才是真正的耦合。
> 如果你在几十个群里、嫌转发流量大，可以在 bot 侧再加一层只读的群过滤，
> 但**别让两边都成为策略的真相来源**。

---

## 1. 它在整个系统里的位置

```
QQ/NapCat ──OneBot WS──▶ xcollector-bot ──HTTP POST──▶ xcollector-backend
                              ▲                              │
                              └────── HTTP POST ─────────────┘
                                 (后端要发 digest 时调用 bot)
```

三个仓库的分工：

| 仓库 | 职责 | 不允许做的事 |
|---|---|---|
| `xcollector-backend` | 抽取、存储、digest 调度、HTTP API、附件落地 | 不碰 OneBot 协议 |
| `xcollector-web` | 展示 | 不碰 OneBot 协议 |
| **`xcollector-bot`**（本仓库） | OneBot 连接、消息归一化、合并转发展开、私聊指令 | **没有数据库**，不做抽取，不存附件 |

一句话概括边界：**"OneBot 长什么样"这件事只存在于本仓库**。
backend 收到的是 `{"source":"qq","text":"...","attachments":[...]}` 这种协议无关的结构。

数据流上的两个方向：

- **QB → 后端（消息）**：`bot` 只推**群消息**。私聊消息由 bot 自己处理（指令），不推给后端。
- **后端 → bot（发送）**：backend 要发 digest 时，调 `POST /api/send/private`。
  bot 自己没有定时任务，什么时候发完全由 backend 决定。

---

## 2. 快速开始

```powershell
cd E:\Xcollector\xcollector-bot

# 1) 建虚拟环境（Python 3.12+；本机用 3.14 验证过）
python -m venv .venv

# 2) 装依赖
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 3) 配置
Copy-Item .env.example .env
#    然后编辑 .env：至少确认 ONEBOT_MODE / ONEBOT_WS_URL / BACKEND_BASE_URL

# 4) 启动
.\.venv\Scripts\python.exe -m app.main
# 或
.\.venv\Scripts\uvicorn.exe app.main:app --host 127.0.0.1 --port 8082
```

启动后：

- 本服务的 HTTP API：`http://127.0.0.1:8082/api/status`
- 交互式文档：`http://127.0.0.1:8082/docs`

**没有任何配置也能启动**：所有项都有默认值。没连上 NapCat 时 `/api/status` 会返回
`connected: false` 并带上 `last_error`，这本身就是一个可用的自检结果。

> **本机（E:\Xcollector）装依赖的坑**：这台机器的沙箱会阻止 pip 写临时文件，
> 表现是 `python -m venv .venv` 报 `ensurepip` 失败、venv 里没有 pip。
> 绕法是先把 miniconda 的 pip 拷进 venv，再让它用 `os.makedirs` 建临时目录：
>
> ```powershell
> Copy-Item -Recurse D:\miniconda\Lib\site-packages\pip* .\.venv\Lib\site-packages\ -Force
>
> @'
> import os, sys, tempfile
> def _mkdtemp(suffix=None, prefix=None, dir=None):
>     parent = dir or tempfile.gettempdir()
>     os.makedirs(parent, exist_ok=True)
>     path = os.path.join(parent, (prefix or "tmp") + os.urandom(8).hex())
>     os.makedirs(path, exist_ok=True)
>     return path
> tempfile.mkdtemp = _mkdtemp          # 沙箱下 mkdtemp 建的目录不可写，换成 makedirs
> from pip._internal.cli.main import main
> sys.exit(main(sys.argv[1:]))
> '@ | Set-Content -Encoding UTF8 .pippatch.py
>
> .\.venv\Scripts\python.exe .pippatch.py install --no-cache-dir -r requirements.txt
> ```
>
> 依赖装好之后，后面所有命令都正常。换台机器就不用管这一段。

### 两个端口，别搞混（最常见的配置事故）

| 环境变量 | 默认 | 谁连谁 | 什么时候用 |
|---|---|---|---|
| `ONEBOT_LISTEN_PORT` | `8081` | **NapCat → bot**（反向 WS） | 只在 `ONEBOT_MODE=server` 时监听 |
| `BOT_LISTEN_PORT` | `8082` | **backend → bot**（HTTP） | 一直监听 |

也就是说：`8081` 是"QQ 消息进来的门"，`8082` 是"后端下命令的门"。
`ONEBOT_MODE=client` 时 8081 **根本不会打开**（此时 bot 是主动去连 NapCat 的那个端口）。

---

## 3. NapCat 两种模式

### 模式 A：`ONEBOT_MODE=client`（推荐，默认）

bot 主动连 NapCat。

`.env`：

```env
ONEBOT_MODE=client
ONEBOT_WS_URL=ws://127.0.0.1:3001
ONEBOT_ACCESS_TOKEN=
```

NapCat 那边：**网络配置 → 新建 → WebSocket 服务器**

| NapCat 配置项 | 值 |
|---|---|
| 主机 | `0.0.0.0` 或 `127.0.0.1` |
| 端口 | `3001` ← **必须和 `ONEBOT_WS_URL` 里的端口一致** |
| Token | 留空，或和 `ONEBOT_ACCESS_TOKEN` 一致 |
| 启用 | ✅ |

### 模式 B：`ONEBOT_MODE=server`（反向 WS）

bot 开一个 WS 端口，NapCat 连过来。适合 bot 在内网、NapCat 在外网/另一台机器的情况。

`.env`：

```env
ONEBOT_MODE=server
ONEBOT_LISTEN_HOST=0.0.0.0
ONEBOT_LISTEN_PORT=8081
ONEBOT_LISTEN_PATH=/onebot/ws
ONEBOT_ACCESS_TOKEN=
```

NapCat 那边：**网络配置 → 新建 → WebSocket 客户端**

| NapCat 配置项 | 值 |
|---|---|
| URL | `ws://<bot所在机器IP>:8081/onebot/ws` ← 端口和路径都要对上 |
| Token | 留空，或和 `ONEBOT_ACCESS_TOKEN` 一致 |
| 启用 | ✅ |

两种模式的**下游代码完全一样**（`hub.py` 里的 `_Conn` 把 websockets 和 Starlette 的 WebSocket
归一化成 `send_text`/`recv_text`），所以随时可以切换，不用改任何业务代码。

> server 模式下进程会**同时监听两个端口**：`ONEBOT_LISTEN_PORT` 上只服务反向 WS
> （单独起的一个只含一条路由的小应用），`BOT_LISTEN_PORT` 上才是给 backend 的 HTTP API。
> 两个 server 跑在同一个事件循环里 —— hub 的连接对象是 asyncio 原语，跨循环用会坏。
>
> 换模式后记得重启本服务：反向 WS 的监听只在 `ONEBOT_MODE=server` 时启动。

---

## 4. 指令

只对 `COMMAND_WHITELIST` 里的 QQ 号生效，**其他人发的私聊消息直接忽略**（只记 DEBUG 日志，不回复）。

```env
COMMAND_WHITELIST=242684313:我,10001:李老师
```

前缀 `COMMAND_PREFIX` 默认 `/`，同时兼容全角 `／`、前后空白、指令名大小写。

| 指令 | 作用 |
|---|---|
| `/help` | 显示指令列表 |
| `/add <内容>` | 添加任务（核心功能） |
| `/list [n]` | 查看待办，默认前 10 条 |
| `/done <编号>` | 标记完成 |
| `/del <编号>` | 移除（"这不是通知" / "加错了"） |
| `/cancel` | 取消待确认的添加 |

### `/add`

```
/add 明天下午3点 交实验报告
/add 下周三前把体检表交到学工办
/add 本周五19:00 教三201 开班会
```

- 后端能解析出截止时间 → 直接建好并回执：

  ```
  ✅ 已添加：交实验报告
  🕒 截止：09-17 15:00（明天下午3点）
  📍 地点：教三201
  🔖 编号：#3f9a2c
  ```

- 后端解析不出截止时间 → **不建任务**，把理解结果回显给用户确认（10 分钟内有效）：

  ```
  ⚠️ 我没解析出明确的截止时间。
  我理解为：尽快把材料交上来
  截止：未识别
  仍然添加吗？回复 y 确认，n 取消。
  ```

  回 `y` / `yes` / `是` / `确认` → 以 `force_commit` 再调一次，建好后回执；
  回 `n` / `no` / `否` / `取消` → 丢弃，回复「已取消」。
  中间发别的指令（比如 `/cancel`）→ 同样丢弃。

  为什么要多这一步：`/add 尽快交` 这种句子**没有 DDL**，
  猜测一个时间建出任务是错的（用户会看到一条假的截止时间），
  直接拒绝又是无用的（用户明明想加）。让用户确认一次是唯一不撒谎的做法。

### `/list`

```
📋 待办 8 条（显示前 10）
1. 交实验报告        09-17 15:00  教三201
2. 提交军训心得      ~09-23 23:59
3. 安全教育平台学习  待确认
用 /done <编号> 标记完成，/del <编号> 移除
```

- 编号从 1 开始，**按用户单独记**：A 的"3 号"和 B 的"3 号"是各自看到的那条（内存映射，重启即失）。
- `due_at` 为空时退回 `due_text` 原文；都没有写「待确认」，不会出现空白。
- 置信度 0.6~0.9 的时间前面加 `~`（意思是"大概是这个点"）。
- 跨年的时间带年份（`2027-01-05 23:59`），避免"01-05"让人误判月份。
- 用 `-` 表示"**不是通知**"：`/del 3` 会把第 3 条归档，比 `done` 更彻底。

### `/done` `/del`

```
/done 1     → ✅ 已完成：交实验报告
/del 2      → 🗑 已移除：提交军训心得
```

对应后端 `POST /api/notifications/{id}/corrections`，
`user_id` 固定写成 `qq:<QQ号>` —— 这样在 backend 的修正记录里能分清
"是网页改的"还是"是哪个人在 QQ 上发的指令"。

> ⚠️ `/done` 会写入 `status=done`。如果后端还停留在只认 `active/archived` 的版本，
> 这里会返回「操作被后端拒绝」。升级顺序：**先让 backend 支持 `done`，再上线 bot**。

### 无参数 / 未知指令

- `/add` 不带内容 → 回用法示例。
- `/done` `/del` 不带编号 → 提示先发 `/list`。
- `未知指令，发 /help 查看可用指令`。

### 后端不可用时

指令会回复「后端暂时不可用，请稍后再试」，不会静默失败、也不会抛异常。
**指令不做退避重试**（只试一次）：用户在线等着，让他在 7 秒的退避里干等
比直接告诉他"稍后再试"更糟。转发（不含用户等待）才用 1s/2s/4s 的退避。

---

## 5. 接口契约

### 5.1 后端 → bot（bot 暴露的 HTTP API）

所有请求带 `Authorization: Bearer <BOT_API_TOKEN>`。
`BOT_API_TOKEN` 为空时**跳过校验**（仅本地开发，启动时打 WARNING）。

| 方法 | 路径 | 请求 | 响应 |
|---|---|---|---|
| POST | `/api/send/private` | `{"user_id": "10001", "message": "..."}` | `{"ok": true, "error": null}` |
| POST | `/api/send/group` | `{"group_id": "123", "message": "..."}` | `{"ok": true, "error": null}` |
| GET | `/api/status` | — | 见下 |

**发送失败不返回 5xx**，而是 200 + `{"ok": false, "error": "OneBot 未连接：..."}`。
这样 backend 能区分：

- HTTP 4xx/5xx = **调用失败**（bot 自己的问题：token 不对、请求体不合法）；
- `ok: false` = **发送失败**（bot 收到了，但发不出去：NapCat 没连、QQ 那边拒绝）。

`message` 超过 1500 字符会被截断并加后缀 `…（已截断）`。

`GET /api/status`：

```json
{
  "connected": true,
  "mode": "client",
  "target": "ws://127.0.0.1:3001",
  "last_event_at": 1757692790000,
  "reconnect_count": 0,
  "last_error": null,
  "groups": [
    {"group_id": "673504310", "group_name": "NOVA官方通知群", "last_msg_ts": 1757692790000}
  ]
}
```

- `last_event_at` 是**事件**时间（含心跳），回答的是"WS 还通吗"。
- `groups` 是**bot 见过的群**（收到过消息的），群名靠 `get_group_info` 查一次后缓存。
  这是拆分之后 backend 唯一能知道"bot 到底看得见哪些群"的途径 ——
  digest 之前可以先用它对照一下群白名单。

### 5.2 bot → 后端

基础地址 `BACKEND_BASE_URL`，认证头 `Authorization: Bearer <BACKEND_API_TOKEN>`（为空则不带）。

#### `POST /api/ingest/messages`

```json
{
  "messages": [
    {
      "source": "qq",
      "message_id": "12345",
      "group_id": "673504310",
      "group_name": "NOVA官方通知群",
      "sender_id": "10001",
      "sender_name": "李老师",
      "ts": 1757692800000,
      "text": "@全体成员 大家下周三前把军训心得交到班长那里，不少于800字。\n[图片]\n[合并转发内容]\n  <张三> 军训心得下周三前交，注意格式",
      "at_all": true,
      "mentions": [],
      "reply_to": null,
      "attachments": [
        {"type": "image", "url": "https://...", "name": "x.jpg", "size": 12345}
      ],
      "raw": {"post_type": "message", "……原始 OneBot 事件，仅作留档……": true}
    }
  ]
}
```

约定：

- `ts` 是**毫秒**（OneBot 的 `time` 是秒，在 bot 里已转换）。
- `text` 是**已经递归展开合并转发之后**的纯文本。backend 永远不需要知道
  OneBot 的消息段、CQ 码、`get_forward_msg`。
- `attachments` 只有 `{type, url, name, size}` 四个字段。
  **bot 不下载文件**：URL 有时效性，谁负责落地、存哪里、留多久都是 backend 的事。
- `group_name` / `sender_name` 尽量填（`sender.card` 优先于 `sender.nickname`）；
  群名查不到时为 `null`。
- 转发是**单向**的：不等响应影响消息接收。非 2xx 或超时 → WARNING + 重试 3 次
  （退避 1s/2s/4s），仍失败 → ERROR 并丢弃这一批，**绝不阻塞接收循环**。

`BACKEND_MAX_RETRIES`（默认 3）= 首次尝试之外的重试次数，即总共最多 4 次尝试。

#### 指令用到的接口

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/tasks/manual` | `/add` |
| GET | `/api/notifications?status=active&limit=50` | `/list` |
| POST | `/api/notifications/{id}/corrections` | `/done` `/del` |

`/add` 请求体：

```json
{"text": "明天下午3点 交实验报告", "sender_id": "10001", "sender_name": "李老师", "auto_commit": true}
```

确认后再调一次时：`{"text": "<同一段原文>", "auto_commit": false, "force_commit": true}`。

`/done` `/del` 请求体：

```json
{"field": "status", "value": "done", "user_id": "qq:10001"}
{"field": "status", "value": "archived", "user_id": "qq:10001"}
```

---

## 6. 端到端自检（没有 NapCat、没有 backend 也能验证）

拆成三个进程之后，"消息没进来"可能是 NapCat、bot、backend 任何一段的问题。
下面这套工具就是用来**把三段分别测出来**的。

### 6.1 bot 自己活着吗

```powershell
# 状态（NapCat 没连时 connected=false，这本身也是有效信息）
Invoke-RestMethod http://127.0.0.1:8082/api/status | ConvertTo-Json -Depth 5

# 让 bot 发一条私聊：NapCat 未连接时应当返回 ok=false，而**不是 500**
Invoke-RestMethod -Method Post http://127.0.0.1:8082/api/send/private `
  -ContentType 'application/json' `
  -Body '{"user_id":"10001","message":"[自检] hello"}'
# → {"ok": false, "error": "OneBot 未连接：OneBot 未连接"}
```

或者用工具（还会顺便检查"是不是 200"）：

```powershell
.\.venv\Scripts\python.exe -m app.tools.send_test --to-self
```

### 6.2 归一化对不对（不需要 NapCat）

```powershell
# 只看结果，不发网络请求：会打印推送用的完整 JSON，含递归展开后的合并转发
.\.venv\Scripts\python.exe -m app.tools.send_test --print-only

# 模拟 get_forward_msg 超时，验证降级成 [合并转发展开失败] 而不是丢消息
.\.venv\Scripts\python.exe -m app.tools.send_test --print-only --forward-fail
```

`send_test` 里的 `FakeHub` 顶替真实连接，把 `get_forward_msg` / `get_group_info`
换成固定数据 —— 所以**合并转发递归展开、群名缓存这些逻辑都是真的在跑**。

### 6.3 bot 收到消息这条链路会不会崩（不需要 NapCat）

```powershell
# 把假事件直接喂进 handle_event：归一化 → 攒批 → 转发 → 群列表登记
.\.venv\Scripts\python.exe -m app.tools.feed_event
.\.venv\Scripts\python.exe -m app.tools.feed_event --count 3 --wait 2
```

它和 `send_test` 的区别：`send_test` 测的是"归一化 + 调 backend"，
`feed_event` 测的是**真正收到消息时走的那条路**（含队列、攒批、群列表登记），
所以它能回答"NapCat 没连上时收到消息会不会崩"。

### 6.4 转发 + 重试（需要一个"本地假后端"）

真 backend 没起来的时候，用假后端：

```powershell
# 终端 A：假后端，前 2 次 ingest 调用故意返回 500
.\.venv\Scripts\python.exe -m app.tools.fake_backend --port 9000 --fail-first 2

# 终端 B：推一条假消息过去
.\.venv\Scripts\python.exe -m app.tools.send_test --backend-url http://127.0.0.1:9000
```

终端 B 的日志里应该能看到两次 `调用后端失败……1s 后重试` / `2s 后重试`，最后 `[OK]`；
终端 A 会打印它到底收到了什么：

```powershell
Invoke-RestMethod http://127.0.0.1:9000/api/_fake/state | ConvertTo-Json -Depth 6
```

假后端还实现了 `/api/tasks/manual`、`/api/notifications`、`/corrections`，
所以把它配进 `.env`（`BACKEND_BASE_URL=http://127.0.0.1:9000`）之后，
可以用另一个 QQ 号给 bot 发私聊，把 `/add` `/list` `/done` 整条链路走一遍。

### 6.5 指令解析（完全离线）

```powershell
.\.venv\Scripts\python.exe -m tests.check_commands
```

纯函数测试：全角斜杠、大小写、无参数、未知指令、编号解析、时间排版、截断、
`y/n` 词表、pending 过期、事件分类。不连任何东西。

### 6.6 语法检查

```powershell
.\.venv\Scripts\python.exe -m compileall -q app tests
```

---

## 7. 已知边界

1. **没有数据库、没有持久化**。待确认的 `/add`（10 分钟）和 `/list` 的编号映射都在内存里，
   进程重启即失。用户重发一次即可 —— 换来的是 bot 可以随便重启、随便多开。
2. **只推群消息**。私聊消息只用于指令；`discuss`（讨论组）暂不支持，收到只记 DEBUG。
3. **转发失败即丢弃**。重试 3 次仍失败就记 ERROR 并丢，不做本地落盘补偿。
   队列上限 2000 条，满了丢**最旧**的（新消息更接近"现在"）。
   如果需要"一条都不能少"，那应该在 backend 侧做，而不是让 bot 变成有状态的存储。
4. **`file` 类型的附件可能拿不到 URL**。NapCat 对文件段有时只给本地路径 `file`，
   而那个路径在 backend 机器上没有意义，所以 `url` 会是 `null`，`name` 仍然保留。
5. **合并转发有深度上限**（`FORWARD_MAX_DEPTH=3`）。超深时不再展开，
   会留下 `[合并转发层级超过上限，已省略]` 占位符（不丢消息，但内容不全）。
6. **群名查询有 5 分钟负缓存**。群号配错或机器人已退群时，`/api/status` 里该群
   `group_name` 会是 `null`，这是刻意的：否则每条消息都要去等一次必然失败的 API 调用。
7. **`/done` 依赖后端支持 `status=done`**。当前 backend 若只认 `active/archived`，
   用户会收到「操作被后端拒绝」。
8. **时间渲染固定 UTC+8**（`Asia/Shanghai`）。没有做成配置项：全系统一个时区
   才不会出现"digest 说 21:30、列表说 13:30"。
9. **单进程单连接**。同时跑两份 bot 会导致 NapCat 收到两份消息（重复入库）——
   一个 QQ 号只应该有一个 bot 实例。
