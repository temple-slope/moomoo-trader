"""NewsProvider Protocol定義"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class NewsProvider(Protocol):
    """ニュースソースの共通インターフェース"""

    @property
    def name(self) -> str:
        """プロバイダ名 (例: "google_news", "x")"""
        ...

    def fetch_articles(self, query: str, max_count: int = 50) -> list[dict[str, Any]]:
        """正規化済み記事リストを返す。

        各レコードは以下のキーを持つ:
            article_id, provider, title, url, source, summary, query, published_at, raw_json
        """
        ...

    def close(self) -> None:
        """リソース解放"""
        ...
