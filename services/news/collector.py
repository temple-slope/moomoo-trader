"""ニュース収集 + Redis通知"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

import redis

from config import FETCH_DELAY, REDIS_HOST, REDIS_PORT, STOCK_KEYWORDS, X_INTERVAL
from db import upsert_articles
from providers.base import NewsProvider

logger = logging.getLogger(__name__)


class NewsCollector:
    """複数プロバイダからニュースを収集"""

    def __init__(
        self,
        providers: dict[str, NewsProvider],
        conn: sqlite3.Connection,
        queries: list[str],
    ) -> None:
        self.providers = providers
        self.conn = conn
        self.queries = queries
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self._last_fetch: dict[str, float] = {}
        self._provider_intervals: dict[str, int] = {"x": X_INTERVAL}

    def _publish(self, channel: str, data: dict[str, Any]) -> None:
        message = json.dumps(data, ensure_ascii=False)
        self.redis.publish(channel, message)
        logger.debug("publish %s", channel)

    @staticmethod
    def _is_stock_relevant(article: dict[str, Any]) -> bool:
        """株価に影響する記事かキーワードで判定"""
        text = (article.get("title", "") + " " + article.get("summary", "")).lower()
        return any(kw in text for kw in STOCK_KEYWORDS)

    def _should_fetch(self, provider_name: str) -> bool:
        """プロバイダ固有の間隔を超えたか判定"""
        interval = self._provider_intervals.get(provider_name)
        if interval is None:
            return True
        last = self._last_fetch.get(provider_name, 0)
        return (time.time() - last) >= interval

    def run_once(self) -> None:
        """全プロバイダ × 全クエリで記事を収集（株価関連のみ保存）"""
        for provider_name, provider in self.providers.items():
            if not self._should_fetch(provider_name):
                continue
            for query in self.queries:
                try:
                    articles = provider.fetch_articles(query)
                    articles = [a for a in articles if self._is_stock_relevant(a)]
                    count = upsert_articles(self.conn, articles)
                    if count > 0:
                        self._publish(
                            "news:articles:update",
                            {
                                "provider": provider_name,
                                "query": query,
                                "count": count,
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            },
                        )
                except Exception:
                    logger.exception("収集失敗: provider=%s, query=%s", provider_name, query)
                time.sleep(FETCH_DELAY)
            self._last_fetch[provider_name] = time.time()

    def close(self) -> None:
        for p in self.providers.values():
            p.close()
        self.redis.close()
