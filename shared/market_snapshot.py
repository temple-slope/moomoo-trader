"""マーケットスナップショット生成

SQLiteの生OHLCVデータからAI読み取り用の構造化スナップショットを生成する。
テクニカル指標・変化率・トレンド判定を含む。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import pandas as pd

# 銘柄マスタ: yfinanceティッカー → (名称, カテゴリ)
TICKER_MASTER: dict[str, tuple[str, str]] = {
    # 指数
    "^N225": ("日経225", "index"),
    "^GSPC": ("S&P500", "index"),
    "^DJI": ("NYダウ", "index"),
    "^IXIC": ("NASDAQ", "index"),
    "^HSI": ("ハンセン指数", "index"),
    "^FTSE": ("FTSE100", "index"),
    "^GDAXI": ("DAX", "index"),
    # コモディティ
    "GC=F": ("金先物", "commodity"),
    "SI=F": ("銀先物", "commodity"),
    "CL=F": ("WTI原油", "commodity"),
    "NG=F": ("天然ガス", "commodity"),
    "HG=F": ("銅先物", "commodity"),
    # 為替 (参考)
    "JPY=X": ("USD/JPY", "fx"),
    "EURJPY=X": ("EUR/JPY", "fx"),
    # moomoo / J-Quants
    "HK.00700": ("テンセント", "stock"),
}


@dataclass
class TechnicalIndicators:
    """テクニカル指標"""

    sma_5: float | None = None
    sma_25: float | None = None
    sma_75: float | None = None
    rsi_14: float | None = None
    atr_14: float | None = None
    bollinger_upper: float | None = None
    bollinger_lower: float | None = None
    macd: float | None = None
    macd_signal: float | None = None
    volume_sma_5: float | None = None


@dataclass
class TickerSnapshot:
    """1銘柄のスナップショット"""

    code: str
    name: str
    category: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    prev_close: float | None
    change: float | None
    change_pct: float | None
    day_range_pct: float | None
    trend: str  # "bullish" / "bearish" / "neutral"
    indicators: TechnicalIndicators

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # float を丸める
        for key in ("open", "high", "low", "close", "prev_close", "change"):
            if d.get(key) is not None:
                d[key] = round(d[key], 2)
        if d.get("change_pct") is not None:
            d["change_pct"] = round(d["change_pct"], 2)
        if d.get("day_range_pct") is not None:
            d["day_range_pct"] = round(d["day_range_pct"], 2)
        # indicators も丸める
        ind = d.get("indicators", {})
        for k, v in ind.items():
            if isinstance(v, float):
                ind[k] = round(v, 2)
        return d


@dataclass
class MarketSnapshot:
    """マーケット全体のスナップショット"""

    generated_at: str
    tickers: list[TickerSnapshot]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "summary": self._build_summary(),
            "tickers": [t.to_dict() for t in self.tickers],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def _build_summary(self) -> dict[str, Any]:
        """AI向けサマリー"""
        by_category: dict[str, list[TickerSnapshot]] = {}
        for t in self.tickers:
            by_category.setdefault(t.category, []).append(t)

        summary: dict[str, Any] = {}
        for cat, items in by_category.items():
            with_change = [t for t in items if t.change_pct is not None]
            bullish = [t for t in items if t.trend == "bullish"]
            bearish = [t for t in items if t.trend == "bearish"]
            summary[cat] = {
                "count": len(items),
                "bullish": len(bullish),
                "bearish": len(bearish),
                "top_gainer": max(with_change, key=lambda x: x.change_pct).code
                if with_change
                else None,
                "top_loser": min(with_change, key=lambda x: x.change_pct).code
                if with_change
                else None,
            }
        return summary


def _compute_technicals(df: pd.DataFrame) -> TechnicalIndicators:
    """DataFrameからテクニカル指標を計算 (古い順にソート済み前提)"""
    ind = TechnicalIndicators()
    close = df["close"]
    n = len(close)

    # SMA
    if n >= 5:
        ind.sma_5 = float(close.tail(5).mean())
    if n >= 25:
        ind.sma_25 = float(close.tail(25).mean())
    if n >= 75:
        ind.sma_75 = float(close.tail(75).mean())

    # RSI (14)
    if n >= 15:
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0).tail(14)
        loss = (-delta.where(delta < 0, 0.0)).tail(14)
        avg_gain = gain.mean()
        avg_loss = loss.mean()
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            ind.rsi_14 = float(100 - 100 / (1 + rs))
        else:
            ind.rsi_14 = 100.0

    # ATR (14)
    if n >= 15:
        high = df["high"]
        low = df["low"]
        prev_close = close.shift(1)
        tr = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        ind.atr_14 = float(tr.tail(14).mean())

    # Bollinger Bands (20, 2σ)
    if n >= 20:
        sma_20 = float(close.tail(20).mean())
        std_20 = float(close.tail(20).std())
        ind.bollinger_upper = sma_20 + 2 * std_20
        ind.bollinger_lower = sma_20 - 2 * std_20

    # MACD (12, 26, 9)
    if n >= 35:
        ema_12 = close.ewm(span=12, adjust=False).mean()
        ema_26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        ind.macd = float(macd_line.iloc[-1])
        ind.macd_signal = float(signal_line.iloc[-1])

    # 出来高SMA (5)
    vol = df["volume"]
    if n >= 5:
        ind.volume_sma_5 = float(vol.tail(5).mean())

    return ind


def _determine_trend(
    close: float, indicators: TechnicalIndicators
) -> str:
    """SMA + RSI からトレンドを簡易判定"""
    signals = 0

    # 価格 vs SMA
    if indicators.sma_5 is not None:
        signals += 1 if close > indicators.sma_5 else -1
    if indicators.sma_25 is not None:
        signals += 1 if close > indicators.sma_25 else -1

    # RSI
    if indicators.rsi_14 is not None:
        if indicators.rsi_14 > 60:
            signals += 1
        elif indicators.rsi_14 < 40:
            signals -= 1

    # MACD
    if indicators.macd is not None and indicators.macd_signal is not None:
        signals += 1 if indicators.macd > indicators.macd_signal else -1

    if signals >= 2:
        return "bullish"
    elif signals <= -2:
        return "bearish"
    return "neutral"


def generate_snapshot(
    conn: sqlite3.Connection,
    codes: list[str] | None = None,
    timeframe: str = "K_DAY",
    history_days: int = 100,
) -> MarketSnapshot:
    """SQLiteからスナップショットを生成

    codes: 対象銘柄リスト。None なら DB 内の全銘柄。
    """
    if codes is None:
        cursor = conn.execute("SELECT DISTINCT code FROM klines WHERE timeframe = ?", (timeframe,))
        codes = [row[0] for row in cursor.fetchall()]

    tickers: list[TickerSnapshot] = []

    for code in sorted(codes):
        df = pd.read_sql_query(
            """
            SELECT timestamp, open, high, low, close, volume, turnover
            FROM klines
            WHERE code = ? AND timeframe = ?
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            conn,
            params=(code, timeframe, history_days),
        )

        if df.empty:
            continue

        latest = df.iloc[-1]
        prev_close = float(df.iloc[-2]["close"]) if len(df) >= 2 else None

        close_val = float(latest["close"])
        change = close_val - prev_close if prev_close else None
        change_pct = (change / prev_close * 100) if (prev_close and prev_close != 0) else None

        high_val = float(latest["high"])
        low_val = float(latest["low"])
        day_range_pct = ((high_val - low_val) / low_val * 100) if low_val != 0 else None

        name, category = TICKER_MASTER.get(code, (code, "other"))
        indicators = _compute_technicals(df)
        trend = _determine_trend(close_val, indicators)

        tickers.append(
            TickerSnapshot(
                code=code,
                name=name,
                category=category,
                timestamp=str(latest["timestamp"]),
                open=float(latest["open"]),
                high=high_val,
                low=low_val,
                close=close_val,
                volume=int(latest["volume"]),
                prev_close=prev_close,
                change=change,
                change_pct=change_pct,
                day_range_pct=day_range_pct,
                trend=trend,
                indicators=indicators,
            )
        )

    return MarketSnapshot(
        generated_at=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        tickers=tickers,
    )
