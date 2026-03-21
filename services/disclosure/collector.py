"""EDINET開示情報 日次収集 + Redis通知"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any

import redis

from config import DOWNLOAD_DIR, FETCH_DELAY, REDIS_HOST, REDIS_PORT
from db import mark_downloaded, upsert_documents
from edinet_client import EdinetClient

logger = logging.getLogger(__name__)


class DisclosureCollector:
    """EDINET 開示書類の日次収集"""

    def __init__(
        self,
        client: EdinetClient,
        conn: sqlite3.Connection,
        lookback_days: int = 3,
    ) -> None:
        self.client = client
        self.conn = conn
        self.lookback_days = lookback_days
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def _publish(self, channel: str, data: dict[str, Any]) -> None:
        message = json.dumps(data, ensure_ascii=False)
        self.redis.publish(channel, message)
        logger.debug("publish %s", channel)

    def collect_documents(self) -> None:
        """直近N日分の書類一覧を収集"""
        today = datetime.now()
        for i in range(self.lookback_days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            try:
                docs = self.client.get_document_list(date)
                count = upsert_documents(self.conn, docs)
                if count > 0:
                    self._publish(
                        "disclosure:documents:update",
                        {"date": date, "count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                    )
                logger.info("documents %s: %d docs", date, count)
            except Exception:
                logger.exception("書類一覧収集失敗 %s", date)
            time.sleep(FETCH_DELAY)

    def download_new_documents(self) -> None:
        """未ダウンロードの書類をダウンロード"""
        cursor = self.conn.execute(
            "SELECT doc_id FROM documents WHERE downloaded = 0 ORDER BY filing_date DESC LIMIT 50"
        )
        doc_ids = [row[0] for row in cursor.fetchall()]

        for doc_id in doc_ids:
            try:
                path = self.client.download_document(doc_id, DOWNLOAD_DIR, doc_type=5)  # CSV
                mark_downloaded(self.conn, doc_id, "csv", str(path))
                logger.info("ダウンロード完了 %s", doc_id)
            except Exception:
                logger.exception("ダウンロード失敗 %s", doc_id)
            time.sleep(FETCH_DELAY)

    def run_once(self) -> None:
        """書類一覧収集 + ダウンロードを1回実行"""
        self.collect_documents()
        self.download_new_documents()

    def close(self) -> None:
        self.redis.close()
