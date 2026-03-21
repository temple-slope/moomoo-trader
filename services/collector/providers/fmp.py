"""Financial Modeling Prep (FMP) Stable API 経由の Kline プロバイダ"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

FMP_API_BASE = "https://financialmodelingprep.com/stable"

# FMP がサポートするタイムフレームとエンドポイント
_TIMEFRAME_MAP: dict[str, str] = {
    "K_1M": "1min",
    "K_5M": "5min",
    "K_15M": "15min",
    "K_60M": "1hour",
    "K_DAY": "daily",
}

SUPPORTED_TIMEFRAMES = set(_TIMEFRAME_MAP.keys())


class FMPProvider:
    """FMP Stable API から Kline を取得する"""

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("FMP_API_KEY が未設定です")
        self._api_key = api_key
        self._client = create_http_client(base_url=FMP_API_BASE)

    @property
    def name(self) -> str:
        return "fmp"

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """FMP API から OHLCV を取得し、正規化して返す。

        code: FMP 形式のティッカー (例: "AAPL", "7203.T", "^GSPC")
        """
        if timeframe not in _TIMEFRAME_MAP:
            raise ValueError(
                f"FMP 未対応タイムフレーム: {timeframe} "
                f"(対応: {sorted(SUPPORTED_TIMEFRAMES)})"
            )

        interval = _TIMEFRAME_MAP[timeframe]

        if interval == "daily":
            return self._fetch_daily(code, max_count)
        return self._fetch_intraday(code, interval, max_count)

    def _fetch_daily(self, code: str, max_count: int) -> list[dict[str, Any]]:
        """日足データ取得 (Stable API: /historical-price-eod/full)"""
        to_date = datetime.now()
        from_date = to_date - timedelta(days=int(max_count * 1.5) + 10)

        resp = self._client.get(
            "/historical-price-eod/full",
            params={
                "symbol": code,
                "from": from_date.strftime("%Y-%m-%d"),
                "to": to_date.strftime("%Y-%m-%d"),
                "apikey": self._api_key,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning("FMP: %s の日足データなし", code)
            return []

        # FMP は新しい順で返すので、max_count で切ってから古い順にソート
        data = data[:max_count]
        data.reverse()

        return [
            {
                "timestamp": h["date"],
                "open": h.get("open", 0),
                "high": h.get("high", 0),
                "low": h.get("low", 0),
                "close": h.get("close", 0),
                "volume": h.get("volume", 0),
                "turnover": 0,
            }
            for h in data
        ]

    def _fetch_intraday(self, code: str, interval: str, max_count: int) -> list[dict[str, Any]]:
        """分足・時間足データ取得 (Stable API: /historical-chart/{interval})"""
        resp = self._client.get(
            f"/historical-chart/{interval}",
            params={
                "symbol": code,
                "apikey": self._api_key,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning("FMP: %s の%sデータなし", code, interval)
            return []

        # FMP は新しい順で返すので、max_count で切ってから古い順にソート
        data = data[:max_count]
        data.reverse()

        return [
            {
                "timestamp": d.get("date", ""),
                "open": d.get("open", 0),
                "high": d.get("high", 0),
                "low": d.get("low", 0),
                "close": d.get("close", 0),
                "volume": d.get("volume", 0),
                "turnover": 0,
            }
            for d in data
        ]

    def close(self) -> None:
        self._client.close()
