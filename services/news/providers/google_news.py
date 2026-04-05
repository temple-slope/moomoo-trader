"""Google News RSS プロバイダ"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from time import mktime
from typing import Any
from urllib.parse import quote

import feedparser
from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search"


class GoogleNewsProvider:
    """Google News RSS からニュースを取得"""

    def __init__(self) -> None:
        self._client = create_http_client(timeout=15.0)

    @property
    def name(self) -> str:
        return "google_news"

    def fetch_articles(self, query: str, max_count: int = 50) -> list[dict[str, Any]]:
        """RSSフィードを解析して記事リストを返す"""
        url = f"{GOOGLE_NEWS_RSS_BASE}?q={quote(query)}&hl=ja&gl=JP&ceid=JP:ja"

        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except Exception:
            logger.exception("Google News RSS 取得失敗: query=%s", query)
            return []

        feed = feedparser.parse(resp.text)
        articles: list[dict[str, Any]] = []

        for entry in feed.entries[:max_count]:
            article_id = hashlib.sha256(entry.get("link", "").encode()).hexdigest()[:16]
            published_at = ""
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                dt = datetime.fromtimestamp(mktime(entry.published_parsed), tz=timezone.utc)
                published_at = dt.isoformat()

            source_name = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                source_name = entry.source.title

            articles.append({
                "article_id": article_id,
                "provider": self.name,
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "source": source_name,
                "summary": entry.get("summary", ""),
                "query": query,
                "published_at": published_at,
                "raw_json": json.dumps(
                    {"title": entry.get("title", ""), "link": entry.get("link", ""), "source": source_name},
                    ensure_ascii=False,
                ),
            })

        logger.info("Google News: query=%s, %d articles", query, len(articles))
        return articles

    def close(self) -> None:
        self._client.close()
