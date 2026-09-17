"""鉴权：令牌 → 范围（scope）。

和后端 `xcollector-backend/app/auth.py` 是同一套规则，只是这里守的是 bot
**自己**的 API（`/api/status`、`/api/digest/*`、`/api/send/*`）。

为什么 bot 也要分：前端登录页拿的是网页令牌，而它要调 `GET /api/status` 和
`GET /api/digest/preview` 来渲染「系统状态」页。如果那一个令牌同时也能调
`POST /api/send/private`，那任何人拿到网页令牌就能以你的 QQ 号任意发消息。

    BOT_API_TOKEN / API_TOKEN   写入范围 —— bot 的管理令牌，**不给浏览器**
    WEB_API_TOKEN               网页范围 —— 只允许 /api/status 与 /api/digest/preview

刻意**不给**网页范围 `POST /api/digest/send`：那会真的往 QQ 发一条消息。
摘要该由 bot 定时发（`DIGEST_TIME`），网页上不该有"立刻群发"的按钮。

`WEB_API_TOKEN` 留空 = 退回单令牌模式（行为与拆分前一致）。
"""

from __future__ import annotations

import logging
import secrets
from enum import Enum
from typing import Annotated

from fastapi import Depends, Header, HTTPException

from .config import get_settings

logger = logging.getLogger(__name__)

_warned_same_token = False


class Scope(str, Enum):
    WRITE = "write"
    WEB = "web"


def resolve_scope(token: str) -> Scope | None:
    """把令牌换成范围；不认识就返回 None。

    两个令牌都无条件比对（不短路），比对本身是定长的。
    """
    prepared = (token or "").strip()
    if not prepared:
        return None

    settings = get_settings()
    write_token = settings.inbound_token.strip()
    # 网页令牌没配 = 单令牌模式：管理令牌同时当网页令牌用
    web_token = settings.web_api_token.strip() or write_token

    if not write_token and not web_token:
        return None

    is_write = bool(write_token) and secrets.compare_digest(prepared, write_token)
    is_web = bool(web_token) and secrets.compare_digest(prepared, web_token)

    if is_write:
        return Scope.WRITE
    if is_web:
        return Scope.WEB
    return None


async def require_token(
    authorization: Annotated[str | None, Header()] = None,
) -> Scope:
    """bot 自己 /api/* 的入口鉴权。

    为什么用普通依赖而不是中间件：这样每个路由的签名里都能看到"这里要认证"，
    将来加一个不需要认证的探活接口也不会被误伤。
    """
    global _warned_same_token

    settings = get_settings()
    write_token = settings.inbound_token.strip()
    if not write_token:
        # 空令牌 = 本地开发模式，启动时已经打过 WARNING
        return Scope.WRITE

    if (
        not _warned_same_token
        and settings.web_api_token.strip()
        and settings.web_api_token.strip() == write_token
    ):
        _warned_same_token = True
        logger.warning(
            "WEB_API_TOKEN 与 BOT_API_TOKEN/API_TOKEN 相同：网页令牌因此拥有完整权限"
            "（包括以你的身份发消息），这次拆分等于没做。请换一个不同的值。"
        )

    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=401,
            detail="缺少 Authorization: Bearer <令牌>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    scope = resolve_scope(token)
    if scope is None:
        raise HTTPException(
            status_code=401,
            detail="令牌无效",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return scope


async def require_write(
    scope: Annotated[Scope, Depends(require_token)],
) -> Scope:
    """写动作的门槛：只有管理令牌能过。

    挂在这些接口上：`POST /api/send/private`、`POST /api/send/group`、
    `POST /api/digest/send` —— 它们都会**真的往 QQ 发消息**。
    """
    if scope is not Scope.WRITE:
        raise HTTPException(
            status_code=403,
            detail=(
                "该接口只允许持有管理令牌的一方调用。网页令牌只能看状态和预览摘要，"
                "不能发消息。"
            ),
        )
    return scope
