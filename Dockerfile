# bot：消息处理层（OneBot 连接 + 抽取 + 指令 + digest）
#
# 容器是**无状态**的：bot 刻意不持有任何需要跨重启存活的数据
# （待确认、编号映射、digest 记录全在后端），所以不需要挂卷。
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

# 时间相关逻辑（digest 的发送时刻、相对时间解析的锚点）都依赖本地时区
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai

WORKDIR /app

COPY requirements.txt ./
# LLM 抽取走 app/llm/ 里的自研网关，它只依赖 httpx（已经在这里了），
# 所以没有"可选的 LLM 依赖"这回事 —— 一次装完，没有构建开关。
RUN pip install -r requirements.txt

COPY app ./app
COPY tests ./tests

EXPOSE 8082 8081

# 优先探 HTTP 层。注意 /api/status 需要 token，未配则不带头。
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD \
  python -c "import os,sys,urllib.request as u; t=os.environ.get('BOT_API_TOKEN') or os.environ.get('API_TOKEN') or ''; r=u.Request('http://127.0.0.1:8082/api/status',headers={'Authorization':'Bearer '+t} if t else {}); sys.exit(0 if u.urlopen(r,timeout=4).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8082"]
