"""KlineProvider Protocol定義"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class KlineProvider(Protocol):
    """Klineデータソースの共通インターフェース"""

    @property
    def name(self) -> str:
        """プロバイダ名 (例: "moomoo", "jquants")"""
        ...

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """正規化済みレコードを返す。

        各レコードは以下のキーを持つ:
            timestamp, open, high, low, close, volume, turnover
        """
        ...

    def close(self) -> None:
        """リソース解放"""
        ...
