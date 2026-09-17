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
# 核心链路（EXTRACTOR=rule）不依赖 litellm，所以默认只装 core。
# 要启用 LLM 抽取：把下面的 requirements-llm.txt 一起装，
# 或在 compose 里把 BUILD_LLM 设为 1（见 docker-compose.yml）。
ARG BUILD_LLM=0
COPY requirements-llm.txt ./
RUN pip install -r requirements.txt \
    && if [ "$BUILD_LLM" = "1" ]; then pip install -r requirements-llm.txt; fi

COPY app ./app
COPY tests ./tests

EXPOSE 8082 8081

# 优先探 HTTP 层。注意 /api/status 需要 token，未配则不带头。
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD \
  python -c "import os,sys,urllib.request as u; t=os.environ.get('BOT_API_TOKEN') or os.environ.get('API_TOKEN') or ''; r=u.Request('http://127.0.0.1:8082/api/status',headers={'Authorization':'Bearer '+t} if t else {}); sys.exit(0 if u.urlopen(r,timeout=4).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8082"]
