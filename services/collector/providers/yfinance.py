"""yfinance 経由の Kline プロバイダ"""

from __future__ import annotations

import logging
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

# yfinance の interval マッピング
_TIMEFRAME_MAP: dict[str, str] = {
    "K_1M": "1m",
    "K_5M": "5m",
    "K_15M": "15m",
    "K_60M": "60m",
    "K_DAY": "1d",
    "K_WEEK": "1wk",
    "K_MON": "1mo",
}

# interval ごとの最大取得期間 (yfinance制約)
_PERIOD_MAP: dict[str, str] = {
    "1m": "7d",
    "5m": "60d",
    "15m": "60d",
    "60m": "730d",
    "1d": "max",
    "1wk": "max",
    "1mo": "max",
}

SUPPORTED_TIMEFRAMES = set(_TIMEFRAME_MAP.keys())


class YFinanceProvider:
    """yfinance から Kline を取得する"""

    @property
    def name(self) -> str:
        return "yfinance"

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """yfinance から OHLCV を取得し、正規化して返す。

        code: yfinance 形式のティッカー (例: "7203.T", "AAPL", "0700.HK")
        """
        if timeframe not in _TIMEFRAME_MAP:
            raise ValueError(
                f"yfinance 未対応タイムフレーム: {timeframe} "
                f"(対応: {sorted(SUPPORTED_TIMEFRAMES)})"
            )

        interval = _TIMEFRAME_MAP[timeframe]
        period = _PERIOD_MAP[interval]

        ticker = yf.Ticker(code)
        df = ticker.history(period=period, interval=interval)

        if df.empty:
            logger.warning("yfinance: %s のデータなし (interval=%s)", code, interval)
            return []

        # NaN行を除外してから新しい順にソートして max_count で切る
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        df = df.sort_index(ascending=False).head(max_count).sort_index()

        records: list[dict[str, Any]] = []
        for ts, row in df.iterrows():
            records.append(
                {
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                    "turnover": 0,
                }
            )

        return records

    def close(self) -> None:
        pass
