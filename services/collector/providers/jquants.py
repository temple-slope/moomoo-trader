"""J-Quants API 経由の Kline プロバイダ (日足のみ)"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from shared.auth.token_manager import JQuantsTokenManager
from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

JQUANTS_API_BASE = "https://api.jquants.com/v1"

# J-Quantsがサポートするタイムフレーム
SUPPORTED_TIMEFRAMES = {"K_DAY"}


class JQuantsProvider:
    """J-Quants daily_quotes からKlineを取得する"""

    def __init__(self, token_manager: JQuantsTokenManager) -> None:
        self._token_manager = token_manager
        self._client = create_http_client(base_url=JQUANTS_API_BASE)

    @property
    def name(self) -> str:
        return "jquants"

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """J-Quants daily_quotesからKlineを取得し、正規化して返す。

        code: J-Quants形式の銘柄コード (例: "72030" = トヨタ5桁コード)
        timeframe: "K_DAY" のみサポート
        """
        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise ValueError(f"J-QuantsはK_DAYのみ対応: {timeframe}")

        # max_count日分の期間を計算 (余裕を持って1.5倍の営業日)
        to_date = datetime.now()
        from_date = to_date - timedelta(days=int(max_count * 1.5) + 10)

        params: dict[str, Any] = {
            "code": code,
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
        }

        headers = self._token_manager.get_auth_headers()
        resp = self._client.get(
            "/prices/daily_quotes",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()

        quotes = data.get("daily_quotes", [])
        if not quotes:
            logger.warning("J-Quants: %s のデータなし", code)
            return []

        # 新しい順にソートして max_count で切る
        quotes.sort(key=lambda x: x["Date"], reverse=True)
        quotes = quotes[:max_count]

        return [
            {
                "timestamp": q["Date"],
                "open": q.get("AdjustmentOpen", q.get("Open", 0)),
                "high": q.get("AdjustmentHigh", q.get("High", 0)),
                "low": q.get("AdjustmentLow", q.get("Low", 0)),
                "close": q.get("AdjustmentClose", q.get("Close", 0)),
                "volume": q.get("Volume", 0),
                "turnover": q.get("TurnoverValue", 0),
            }
            for q in quotes
        ]

    def close(self) -> None:
        self._client.close()
        self._token_manager.close()
