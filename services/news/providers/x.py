"""X (Twitter) API v2 プロバイダ"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class XProvider:
    """X API v2 (Recent Search) からツイートを取得

    Bearer Token 未設定時は fetch_articles が空リストを返す。
    Free プランでは検索不可のため、Basic ($200/月) か Pay-Per-Use が必要。
    """

    def __init__(self, bearer_token: str = "") -> None:
        self._available = bool(bearer_token)
        self._client = None

        if self._available:
            try:
                import tweepy

                self._client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
                logger.info("X プロバイダ初期化完了")
            except ImportError:
                logger.warning("tweepy が未インストールです")
                self._available = False
            except Exception:
                logger.exception("X プロバイダ初期化失敗")
                self._available = False
        else:
            logger.info("X_BEARER_TOKEN 未設定 - X プロバイダ無効")

    @property
    def name(self) -> str:
        return "x"

    def fetch_articles(self, query: str, max_count: int = 50) -> list[dict[str, Any]]:
        """Recent Search API でツイートを検索"""
        if not self._available or self._client is None:
            return []

        capped = min(max_count, 100)  # API上限

        try:
            response = self._client.search_recent_tweets(
                query=query,
                max_results=max(capped, 10),  # API下限は10
                tweet_fields=["created_at", "author_id", "public_metrics", "source"],
                expansions=["author_id"],
            )
        except Exception:
            logger.exception("X API 検索失敗: query=%s", query)
            return []

        if not response.data:
            logger.info("X: query=%s, 0 tweets", query)
            return []

        # ユーザー情報のマッピング
        users = {}
        if response.includes and "users" in response.includes:
            users = {u.id: u for u in response.includes["users"]}

        articles: list[dict[str, Any]] = []
        for tweet in response.data:
            user = users.get(tweet.author_id)
            username = user.username if user else str(tweet.author_id)

            articles.append({
                "article_id": str(tweet.id),
                "provider": self.name,
                "title": "",
                "url": f"https://x.com/{username}/status/{tweet.id}",
                "source": f"@{username}",
                "summary": tweet.text,
                "query": query,
                "published_at": tweet.created_at.isoformat() if tweet.created_at else "",
                "raw_json": json.dumps(
                    {
                        "id": str(tweet.id),
                        "text": tweet.text,
                        "author": username,
                        "metrics": tweet.public_metrics,
                    },
                    ensure_ascii=False,
                ),
            })

        logger.info("X: query=%s, %d tweets", query, len(articles))
        return articles

    def close(self) -> None:
        pass
