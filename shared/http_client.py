"""共通HTTPクライアント (httpx + リトライ)"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# デフォルトのリトライ設定
DEFAULT_MAX_RETRIES = 3
DEFAULT_TIMEOUT = 30.0


def create_http_client(
    base_url: str = "",
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> httpx.Client:
    """リトライ付きの httpx.Client を生成"""
    transport = httpx.HTTPTransport(retries=max_retries)
    return httpx.Client(
        base_url=base_url,
        headers=headers or {},
        timeout=timeout,
        transport=transport,
    )


def api_get(
    client: httpx.Client,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """GETリクエストを実行し、JSONレスポンスを返す"""
    resp = client.get(path, params=params)
    resp.raise_for_status()
    return resp.json()


def api_post(
    client: httpx.Client,
    path: str,
    json_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """POSTリクエストを実行し、JSONレスポンスを返す"""
    resp = client.post(path, json=json_data)
    resp.raise_for_status()
    return resp.json()
