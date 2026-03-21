"""J-Quants API V2 認証

V2 では API Key をリクエストヘッダー (x-api-key) に付与するだけで認証できる。
V1 のリフレッシュトークン → IDトークン方式は廃止済み。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class JQuantsAuth:
    """J-Quants API V2 認証 (API Key 方式)"""

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("JQUANTS_API_KEY が未設定です")
        self._api_key = api_key

    def get_auth_headers(self) -> dict[str, str]:
        """認証ヘッダーを返す"""
        return {"x-api-key": self._api_key}

    def close(self) -> None:
        """互換性のため維持（V2では不要）"""
        pass
