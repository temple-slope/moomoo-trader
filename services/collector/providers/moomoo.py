"""moomoo OpenD 経由の Kline プロバイダ"""

from __future__ import annotations

import logging
from typing import Any

from moomoo import KLType, OpenQuoteContext, RET_OK

logger = logging.getLogger(__name__)


class MoomooProvider:
    """moomoo APIからKlineを取得する"""

    def __init__(self, quote_ctx: OpenQuoteContext) -> None:
        self._quote_ctx = quote_ctx

    @property
    def name(self) -> str:
        return "moomoo"

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """moomoo APIからKlineを取得し、正規化して返す"""
        kl = getattr(KLType, timeframe)
        ret, data, _ = self._quote_ctx.request_history_kline(
            code, ktype=kl, max_count=max_count
        )
        if ret != RET_OK:
            raise RuntimeError(f"Kline取得失敗 {code} {timeframe}: {data}")

        rows = data.to_dict("records")
        return [
            {
                "timestamp": row["time_key"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "turnover": row.get("turnover", 0),
            }
            for row in rows
        ]

    def close(self) -> None:
        pass  # quote_ctx のライフサイクルは呼び出し側が管理
