"""yfinance経由で米国株データを取得する。"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

UNIVERSE = ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "GOOGL", "AMD"]

# Dual Momentum 用セクターETF + 債券
SECTOR_ETFS = ["XLK", "XLF", "XLV", "XLE", "XLI", "XLC", "XLY", "XLP", "XLB", "XLU", "XLRE"]
BOND_ETF = "BND"

# TOPIX-17 業種別ETF (NEXT FUNDS)
JP_SECTOR_ETFS = [
"1617.T", "1618.T", "1619.T", "1620.T", "1621.T",
"1622.T", "1623.T", "1624.T", "1625.T", "1626.T",
"1627.T", "1628.T", "1629.T", "1630.T", "1631.T",
"1632.T", "1633.T",
]

# Breakout Swing 用: S&P500 高流動性銘柄
SWING_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "GOOGL", "AMD",
    "JPM", "BAC", "GS", "UNH", "JNJ", "XOM", "CVX", "HD", "CRM",
    "NFLX", "AVGO", "LLY", "COST", "WMT", "DIS", "NKE", "COIN",
]


def fetch_intraday(symbols: list[str] | None = None, period: str = "60d", interval: str = "5m") -> dict[str, pd.DataFrame]:
    """複数銘柄の日中データを取得。{symbol: DataFrame} を返す。"""
    symbols = symbols or UNIVERSE
    result = {}
    for sym in symbols:
        ticker = yf.Ticker(sym)
        df = ticker.history(period=period, interval=interval)
        if df.empty:
            continue
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                "Close": "close", "Volume": "volume"})
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df.index = df.index.tz_localize(None) if df.index.tz is None else df.index.tz_convert("America/New_York").tz_localize(None)
        result[sym] = df
    return result


def split_by_day(df: pd.DataFrame) -> list[pd.DataFrame]:
    """DataFrameを日ごとに分割する。"""
    groups = df.groupby(df.index.date)
    return [group for _, group in groups]


def calc_daily_atr(df_day: pd.DataFrame, period: int = 14) -> float:
    """前日までのATR(period)を計算。日中データの場合は当日の5分足で近似。"""
    highs = df_day["high"]
    lows = df_day["low"]
    closes = df_day["close"]
    tr = pd.concat([
        highs - lows,
        (highs - closes.shift(1)).abs(),
        (lows - closes.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=min(period, len(tr))).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else float(tr.mean())


def select_top_volatile(data: dict[str, pd.DataFrame], n: int = 3) -> list[str]:
    """前日の値幅率が高い上位n銘柄を選定。"""
    volatility = {}
    for sym, df in data.items():
        days = split_by_day(df)
        if len(days) < 2:
            continue
        prev_day = days[-2]
        day_range = (prev_day["high"].max() - prev_day["low"].min()) / prev_day["close"].iloc[0]
        avg_volume = prev_day["volume"].mean()
        if avg_volume < 1000:
            continue
        volatility[sym] = day_range
    ranked = sorted(volatility, key=volatility.get, reverse=True)
    return ranked[:n]


def fetch_daily(symbols: list[str] | None = None, period: str = "60d") -> dict[str, pd.DataFrame]:
    """複数銘柄の日足データを取得。{symbol: DataFrame} を返す。"""
    symbols = symbols or UNIVERSE
    result = {}
    for sym in symbols:
        ticker = yf.Ticker(sym)
        df = ticker.history(period=period, interval="1d")
        if df.empty:
            continue
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                "Close": "close", "Volume": "volume"})
        df = df[["open", "high", "low", "close", "volume"]].copy()
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        result[sym] = df
    return result


def fetch_daily_combined(symbols: list[str], period: str = "60d") -> pd.DataFrame:
    """複数銘柄の日足終値を1つのDataFrameに結合。列=銘柄シンボル。"""
    frames = {}
    for sym in symbols:
        ticker = yf.Ticker(sym)
        df = ticker.history(period=period, interval="1d")
        if df.empty:
            continue
        if df.index.tz is not None:
            df.index = df.index.tz_convert("America/New_York").tz_localize(None)
        frames[sym] = df["Close"]
    if not frames:
        return pd.DataFrame()
    combined = pd.DataFrame(frames).dropna()
    return combined
