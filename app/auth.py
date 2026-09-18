"""鉴权：bot **自己**的 API 只认一个管理令牌。

守的是 `/api/status`、`/api/digest/*`、`/api/send/*` 这些接口。

## 为什么只剩一个令牌（原来是两个）

以前有两个范围：管理令牌和 `WEB_API_TOKEN`（网页令牌），因为前端要拿后者去调
`GET /api/status` 和 `GET /api/digest/preview` 渲染「系统状态」页。

多用户改造之后**网页不再持有任何共享令牌**：每个用户拿的是自己的 UserToken，
而 bot 根本不认识 UserToken（那是后端的凭据）。所以"网页范围"这个概念消失了。

顺带这解决了一个更根本的问题：系统状态页是**运营者**视角（里面是所有人的
盲区计数、白名单、OneBot 连接状态），本来就不该让任何一个普通用户看到。
现在它和其它接口一样要管理令牌，拿到 UserToken 的普通用户看不到它。

    BOT_API_TOKEN / API_TOKEN   bot 的管理令牌，**不给浏览器、不给用户**

留空 = 本地开发模式，启动时会打 WARNING。
"""

from __future__ import annotations

import logging
import secrets
from typing import Annotated

from fastapi import Header, HTTPException

from .config import get_settings

logger = logging.getLogger(__name__)


async def require_admin(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """bot 自己 /api/* 的入口鉴权。

    为什么用普通依赖而不是中间件：这样每个路由的签名里都能看到"这里要认证"，
    将来加一个不需要认证的探活接口也不会被误伤。
    """
    settings = get_settings()
    expected = settings.inbound_token.strip()
    if not expected:
        # 空令牌 = 本地开发模式，启动时已经打过 WARNING
        return

    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=401,
            detail="缺少 Authorization: Bearer <令牌>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 定长比较，避免通过响应时间逐字节猜令牌
    if not secrets.compare_digest(token.strip(), expected):
        raise HTTPException(
            status_code=401,
            detail="令牌无效",
            headers={"WWW-Authenticate": "Bearer"},
        )
