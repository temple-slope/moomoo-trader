"""財務データ定期収集 + Redis通知"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

import redis

from config import REDIS_HOST, REDIS_PORT
from db import upsert_listed_info, upsert_statements
from jquants_client import JQuantsFundamentalsClient

logger = logging.getLogger(__name__)


class FundamentalsCollector:
    """J-Quants 財務データの定期収集"""

    def __init__(
        self,
        client: JQuantsFundamentalsClient,
        conn: sqlite3.Connection,
        codes: list[str],
        fetch_delay: float = 1.0,
    ) -> None:
        self.client = client
        self.conn = conn
        self.codes = codes
        self.fetch_delay = fetch_delay
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def _publish(self, channel: str, data: dict[str, Any]) -> None:
        message = json.dumps(data, ensure_ascii=False)
        self.redis.publish(channel, message)
        logger.debug("publish %s", channel)

    def collect_statements(self) -> None:
        """全銘柄の財務情報を収集"""
        for code in self.codes:
            try:
                rows = self.client.get_statements(code)
                count = upsert_statements(self.conn, rows)
                if count > 0:
                    self._publish(
                        f"fundamentals:{code}:statements",
                        {"code": code, "count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                    )
                logger.info("statements %s: %d rows", code, count)
            except Exception:
                logger.exception("statements収集失敗 %s", code)
            time.sleep(self.fetch_delay)

    def collect_listed_info(self) -> None:
        """全銘柄の銘柄情報を収集"""
        for code in self.codes:
            try:
                rows = self.client.get_listed_info(code=code)
                count = upsert_listed_info(self.conn, rows)
                if count > 0:
                    self._publish(
                        f"fundamentals:{code}:info",
                        {"code": code, "count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                    )
                logger.info("listed_info %s: %d rows", code, count)
            except Exception:
                logger.exception("listed_info収集失敗 %s", code)
            time.sleep(self.fetch_delay)

    def run_once(self) -> None:
        """全収集を1回実行"""
        self.collect_statements()
        self.collect_listed_info()

    def close(self) -> None:
        self.redis.close()
