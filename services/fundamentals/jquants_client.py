"""J-Quants API V2 クライアント (財務・銘柄情報)"""

from __future__ import annotations

import logging
from typing import Any

from shared.auth.token_manager import JQuantsAuth
from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

JQUANTS_API_BASE = "https://api.jquants.com/v2"


class JQuantsFundamentalsClient:
    """J-Quants V2 財務・銘柄情報取得クライアント"""

    def __init__(self, auth: JQuantsAuth) -> None:
        self._auth = auth
        self._client = create_http_client(base_url=JQUANTS_API_BASE)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = self._auth.get_auth_headers()
        resp = self._client.get(path, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def get_statements(self, code: str, date_from: str = "", date_to: str = "") -> list[dict]:
        """財務情報 (fins/summary) を取得"""
        params: dict[str, Any] = {"code": code}
        if date_from:
            params["date"] = date_from
        data = self._get("/fins/summary", params=params)
        return data.get("data", [])

    def get_listed_info(self, code: str = "", date: str = "") -> list[dict]:
        """銘柄情報を取得"""
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        data = self._get("/equities/master", params=params)
        return data.get("data", [])

    def get_announcement(self) -> list[dict]:
        """決算発表予定を取得"""
        data = self._get("/equities/earnings-calendar")
        return data.get("data", [])

    def close(self) -> None:
        self._client.close()
