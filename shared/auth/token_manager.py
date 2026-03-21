"""J-Quants API トークン管理

リフレッシュトークン → IDトークンの自動更新を行う。
- リフレッシュトークン: 有効期限1週間 (環境変数で設定)
- IDトークン: 有効期限24時間 (自動更新)
"""

from __future__ import annotations

import logging
import time

import httpx

from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

JQUANTS_API_BASE = "https://api.jquants.com/v1"

# IDトークンの有効期限マージン (期限切れ1時間前に更新)
TOKEN_REFRESH_MARGIN = 3600


class JQuantsTokenManager:
    """J-Quants IDトークンの取得・自動更新"""

    def __init__(self, refresh_token: str) -> None:
        if not refresh_token:
            raise ValueError("JQUANTS_REFRESH_TOKEN が未設定です")
        self._refresh_token = refresh_token
        self._id_token: str | None = None
        self._id_token_expires_at: float = 0
        self._client = create_http_client(base_url=JQUANTS_API_BASE)

    @property
    def id_token(self) -> str:
        """有効なIDトークンを返す。期限切れなら自動更新"""
        if self._id_token is None or time.time() >= self._id_token_expires_at:
            self._refresh_id_token()
        return self._id_token  # type: ignore[return-value]

    def _refresh_id_token(self) -> None:
        """リフレッシュトークンからIDトークンを取得"""
        logger.info("J-Quants IDトークンを更新中...")
        try:
            resp = self._client.post(
                "/token/auth_refresh",
                params={"refreshtoken": self._refresh_token},
            )
            resp.raise_for_status()
            data = resp.json()
            self._id_token = data["idToken"]
            # 24時間有効だが、マージンを持たせて23時間で更新
            self._id_token_expires_at = time.time() + (24 * 3600 - TOKEN_REFRESH_MARGIN)
            logger.info("J-Quants IDトークン更新完了")
        except httpx.HTTPStatusError as e:
            logger.error("J-Quants IDトークン更新失敗: %s", e.response.text)
            raise RuntimeError(f"J-Quants認証失敗: {e.response.status_code}") from e

    def get_auth_headers(self) -> dict[str, str]:
        """認証ヘッダーを返す"""
        return {"Authorization": f"Bearer {self.id_token}"}

    def close(self) -> None:
        self._client.close()
