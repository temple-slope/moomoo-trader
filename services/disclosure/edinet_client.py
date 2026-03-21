"""EDINET API クライアント"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# フィルタ対象の書類種別コード
DOC_TYPE_CODES = {
    "120": "有価証券報告書",
    "140": "四半期報告書",
    "160": "半期報告書",
    "350": "大量保有報告書",
    "360": "大量保有報告書(変更)",
}


class EdinetClient:
    """EDINET API v2 クライアント"""

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("EDINET_API_KEY が未設定です")
        self._api_key = api_key
        self._client = create_http_client(base_url=EDINET_API_BASE, timeout=60.0)

    def get_document_list(
        self,
        date: str,
        doc_type_codes: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """指定日の書類一覧を取得。doc_type_codes でフィルタ可能"""
        params = {
            "date": date,
            "type": 2,  # メタデータ+書類一覧
            "Subscription-Key": self._api_key,
        }
        resp = self._client.get("/documents.json", params=params)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            return []

        filter_codes = doc_type_codes or set(DOC_TYPE_CODES.keys())
        return [
            doc for doc in results
            if doc.get("docTypeCode") in filter_codes
        ]

    def get_document(self, doc_id: str) -> dict[str, Any]:
        """書類メタデータを取得"""
        params = {"Subscription-Key": self._api_key}
        resp = self._client.get(f"/documents/{doc_id}", params=params)
        resp.raise_for_status()
        return resp.json()

    def download_document(self, doc_id: str, save_dir: str, doc_type: int = 1) -> Path:
        """書類をダウンロード。

        doc_type: 1=XBRL, 2=PDF, 3=代替書面, 4=英文XBRL, 5=CSV
        """
        params = {
            "type": doc_type,
            "Subscription-Key": self._api_key,
        }
        resp = self._client.get(f"/documents/{doc_id}", params=params)
        resp.raise_for_status()

        save_path = Path(save_dir) / f"{doc_id}.zip"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.content)
        logger.info("ダウンロード完了: %s", save_path)
        return save_path

    def close(self) -> None:
        self._client.close()
