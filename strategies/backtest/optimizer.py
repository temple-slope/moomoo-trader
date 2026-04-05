"""幅広い銘柄・パラメータで戦略を網羅探索し、有意な優位性を持つ組合せを見つける。"""

import itertools
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass

from .data import fetch_intraday, split_by_day
from .strategies import _ema, _rsi, _vwap, _atr, _bollinger, Signal
from .runner import run_backtest, BacktestResult

REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "reports" / "backtest"
INITIAL_CAPITAL = 3300.0

# ── 拡大ユニバース ──

UNIVERSE_TECH = ["AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "META", "AMZN", "TSLA", "AVGO", "CRM", "ORCL", "INTC", "QCOM", "MU", "NFLX"]
UNIVERSE_FINANCE = ["JPM", "BAC", "GS", "MS", "WFC", "C", "BLK", "SCHW"]
UNIVERSE_HEALTH = ["UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "BMY"]
UNIVERSE_ENERGY = ["XOM", "CVX", "COP", "SLB", "OXY"]
UNIVERSE_CONSUMER = ["WMT", "COST", "HD", "NKE", "SBUX", "MCD", "DIS"]
UNIVERSE_ETF = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "ARKK"]
UNIVERSE_MEME = ["GME", "AMC", "PLTR", "SOFI", "MARA", "COIN", "RIOT"]

ALL_UNIVERSES = {
    "TECH": UNIVERSE_TECH,
    "FINANCE": UNIVERSE_FINANCE,
    "HEALTH": UNIVERSE_HEALTH,
    "ENERGY": UNIVERSE_ENERGY,
    "CONSUMER": UNIVERSE_CONSUMER,
    "ETF": UNIVERSE_ETF,
    "MEME": UNIVERSE_MEME,
}


# ── パラメータ付き戦略 ──

class ParameterizedRSI:
    def __init__(self, period=14, oversold=30, overbought=70, atr_tp=1.5, atr_sl=1.0):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.name = f"RSI({period},{oversold}/{overbought},TP{atr_tp}/SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        rsi = _rsi(df_day["close"], self.period)
        atr = _atr(df_day)
        position = False
        for i in range(max(20, self.period + 2), len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            r = rsi.iloc[i]
            a = atr.iloc[i]
            if pd.isna(r) or pd.isna(a) or a == 0:
                continue
            if not position:
                if r < self.oversold:
                    signals.append(Signal(ts, "BUY", price, f"RSI<{self.oversold}"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if r > self.overbought or price >= entry_price + self.atr_tp * a or price <= entry_price - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "RSI exit"))
                    position = False
        return signals


class ParameterizedORB:
    def __init__(self, opening_bars=6, atr_tp=1.5, atr_sl=1.0, vol_mult=1.0):
        self.opening_bars = opening_bars
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.vol_mult = vol_mult
        self.name = f"ORB({opening_bars}bars,TP{atr_tp}/SL{atr_sl},vol{vol_mult})"

    def generate_signals(self, df_day):
        signals = []
        if len(df_day) < self.opening_bars + 5:
            return signals
        opening = df_day.iloc[:self.opening_bars]
        range_high = opening["high"].max()
        atr = _atr(df_day)
        position = False
        for i in range(self.opening_bars, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]
            if pd.isna(a) or a == 0:
                continue
            if not position:
                vol_ok = df_day["volume"].iloc[i] > df_day["volume"].iloc[:i].mean() * self.vol_mult
                if price > range_high and vol_ok:
                    signals.append(Signal(ts, "BUY", price, "ORB breakout"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + self.atr_tp * a or price <= entry_price - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "ORB exit"))
                    position = False
        return signals


class ParameterizedEMA:
    def __init__(self, fast=9, slow=21, atr_tp=1.5, atr_sl=1.0):
        self.fast = fast
        self.slow = slow
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.name = f"EMA({fast}/{slow},TP{atr_tp}/SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        ema_fast = _ema(df_day["close"], self.fast)
        ema_slow = _ema(df_day["close"], self.slow)
        atr = _atr(df_day)
        position = False
        for i in range(self.slow + 2, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]
            if pd.isna(a) or a == 0:
                continue
            if not position:
                if ema_fast.iloc[i - 1] <= ema_slow.iloc[i - 1] and ema_fast.iloc[i] > ema_slow.iloc[i]:
                    signals.append(Signal(ts, "BUY", price, "EMA cross"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + self.atr_tp * a or price <= entry_price - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "EMA exit"))
                    position = False
        return signals


class ParameterizedVWAP:
    def __init__(self, atr_entry=1.0, atr_tp=0.5, atr_sl=1.5):
        self.atr_entry = atr_entry
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.name = f"VWAP(entry{atr_entry},TP{atr_tp}/SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        vwap = _vwap(df_day)
        atr = _atr(df_day)
        position = False
        for i in range(20, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            v = vwap.iloc[i]
            a = atr.iloc[i]
            if pd.isna(v) or pd.isna(a) or a == 0:
                continue
            if not position:
                prev_price = df_day["close"].iloc[i - 1]
                if prev_price < v - self.atr_entry * a and price > v - self.atr_entry * a:
                    signals.append(Signal(ts, "BUY", price, "VWAP revert"))
                    position = True
            else:
                if price >= v + self.atr_tp * a or price <= v - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "VWAP exit"))
                    position = False
        return signals


class ParameterizedBB:
    def __init__(self, period=20, std_dev=2.0, atr_sl=1.0):
        self.period = period
        self.std_dev = std_dev
        self.atr_sl = atr_sl
        self.name = f"BB({period},{std_dev},SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        mid, upper, lower = _bollinger(df_day["close"], self.period, self.std_dev)
        atr = _atr(df_day)
        position = False
        for i in range(self.period + 2, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]
            if pd.isna(lower.iloc[i]) or pd.isna(a) or a == 0:
                continue
            if not position:
                if price <= lower.iloc[i]:
                    signals.append(Signal(ts, "BUY", price, "BB lower touch"))
                    position = True
            else:
                if price >= mid.iloc[i] or price <= lower.iloc[i] - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "BB exit"))
                    position = False
        return signals


# ── モメンタム+トレンドフィルター (新規戦略) ──

class MomentumWithTrendFilter:
    """日足トレンド方向のみにエントリーするモメンタム戦略。"""
    def __init__(self, rsi_period=10, rsi_entry=40, rsi_exit=60, ema_trend=50, atr_tp=2.0, atr_sl=1.0):
        self.rsi_period = rsi_period
        self.rsi_entry = rsi_entry
        self.rsi_exit = rsi_exit
        self.ema_trend = ema_trend
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.name = f"MomTrend(RSI{rsi_period}<{rsi_entry},EMA{ema_trend},TP{atr_tp}/SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        rsi = _rsi(df_day["close"], self.rsi_period)
        ema = _ema(df_day["close"], self.ema_trend)
        atr = _atr(df_day)
        position = False
        for i in range(max(self.ema_trend + 2, 20), len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            r = rsi.iloc[i]
            a = atr.iloc[i]
            if pd.isna(r) or pd.isna(a) or a == 0 or pd.isna(ema.iloc[i]):
                continue
            uptrend = price > ema.iloc[i]
            if not position:
                if uptrend and r < self.rsi_entry:
                    signals.append(Signal(ts, "BUY", price, "MomTrend pullback"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + self.atr_tp * a or price <= entry_price - self.atr_sl * a or r > self.rsi_exit:
                    signals.append(Signal(ts, "SELL", price, "MomTrend exit"))
                    position = False
        return signals


class VolumeBreakout:
    """出来高急増+価格ブレイクアウト。"""
    def __init__(self, vol_mult=2.0, lookback=20, atr_tp=2.0, atr_sl=1.0):
        self.vol_mult = vol_mult
        self.lookback = lookback
        self.atr_tp = atr_tp
        self.atr_sl = atr_sl
        self.name = f"VolBreak(vol{vol_mult}x,lb{lookback},TP{atr_tp}/SL{atr_sl})"

    def generate_signals(self, df_day):
        signals = []
        atr = _atr(df_day)
        position = False
        for i in range(self.lookback + 2, len(df_day)):
            ts = df_day.index[i]
            price = df_day["close"].iloc[i]
            a = atr.iloc[i]
            if pd.isna(a) or a == 0:
                continue
            avg_vol = df_day["volume"].iloc[i - self.lookback:i].mean()
            high_n = df_day["high"].iloc[i - self.lookback:i].max()
            if not position:
                if df_day["volume"].iloc[i] > avg_vol * self.vol_mult and price > high_n:
                    signals.append(Signal(ts, "BUY", price, "Volume breakout"))
                    position = True
            else:
                entry_price = signals[-1].price if signals else price
                if price >= entry_price + self.atr_tp * a or price <= entry_price - self.atr_sl * a:
                    signals.append(Signal(ts, "SELL", price, "VolBreak exit"))
                    position = False
        return signals


def generate_strategy_variants():
    """パラメータグリッドから戦略バリアントを生成。"""
    strategies = []

    # RSI variants
    for period in [7, 10, 14]:
        for os_ob in [(25, 75), (30, 70), (20, 80)]:
            for tp, sl in [(1.5, 1.0), (2.0, 1.0), (2.0, 0.75), (1.0, 0.5)]:
                strategies.append(ParameterizedRSI(period, os_ob[0], os_ob[1], tp, sl))

    # ORB variants
    for bars in [4, 6, 8]:
        for tp, sl in [(1.5, 1.0), (2.0, 1.0), (2.5, 1.0), (2.0, 0.75)]:
            for vol in [0.8, 1.0, 1.5]:
                strategies.append(ParameterizedORB(bars, tp, sl, vol))

    # EMA variants
    for fast, slow in [(5, 13), (8, 21), (9, 21), (12, 26), (5, 20)]:
        for tp, sl in [(1.5, 1.0), (2.0, 1.0), (2.0, 0.75)]:
            strategies.append(ParameterizedEMA(fast, slow, tp, sl))

    # VWAP variants
    for entry in [0.5, 0.75, 1.0, 1.25]:
        for tp, sl in [(0.5, 1.5), (0.75, 1.0), (1.0, 1.0)]:
            strategies.append(ParameterizedVWAP(entry, tp, sl))

    # BB variants
    for period in [15, 20, 25]:
        for std in [1.5, 2.0, 2.5]:
            for sl in [0.5, 1.0, 1.5]:
                strategies.append(ParameterizedBB(period, std, sl))

    # Momentum + Trend Filter
    for rsi_p in [7, 10, 14]:
        for entry, exit_ in [(35, 55), (40, 60), (45, 65)]:
            for ema in [30, 50]:
                for tp, sl in [(2.0, 1.0), (1.5, 0.75), (2.5, 1.0)]:
                    strategies.append(MomentumWithTrendFilter(rsi_p, entry, exit_, ema, tp, sl))

    # Volume Breakout
    for vol in [1.5, 2.0, 3.0]:
        for lb in [10, 20, 30]:
            for tp, sl in [(2.0, 1.0), (1.5, 0.75), (2.5, 1.0)]:
                strategies.append(VolumeBreakout(vol, lb, tp, sl))

    return strategies


def run_scan(universes_to_scan: list[str] | None = None):
    """全ユニバース×全戦略バリアントをスキャンし、上位結果を報告する。"""
    if universes_to_scan is None:
        universes_to_scan = list(ALL_UNIVERSES.keys())

    strategies = generate_strategy_variants()
    print(f"戦略バリアント数: {len(strategies)}")

    all_results: list[tuple[str, str, BacktestResult]] = []

    for univ_name in universes_to_scan:
        symbols = ALL_UNIVERSES[univ_name]
        print(f"\n{'='*60}")
        print(f"ユニバース: {univ_name} ({', '.join(symbols)})")
        print(f"{'='*60}")

        print("データ取得中...")
        data = fetch_intraday(symbols)
        fetched = list(data.keys())
        print(f"取得完了: {fetched}")
        if not data:
            print("  → データなし、スキップ")
            continue

        # 個別銘柄ごとにテスト
        for sym in fetched:
            sym_data = {sym: data[sym]}
            for strat in strategies:
                result = run_backtest(strat, sym_data, initial_capital=INITIAL_CAPITAL, symbols=[sym])
                if result.trade_count >= 10:  # 最低10トレードで統計的に意味あり
                    all_results.append((univ_name, sym, result))

        # ユニバース上位3銘柄でもテスト
        for strat in strategies:
            result = run_backtest(strat, data, initial_capital=INITIAL_CAPITAL)
            if result.trade_count >= 10:
                all_results.append((univ_name, "TOP3_MIX", result))

        # 途中経過: このユニバースの上位5
        univ_results = [(u, s, r) for u, s, r in all_results if u == univ_name]
        if univ_results:
            univ_results.sort(key=lambda x: x[2].total_return_pct, reverse=True)
            print(f"\n  --- {univ_name} 上位5 ---")
            for u, s, r in univ_results[:5]:
                print(f"  {s:<8} | {r.strategy_name:<50} | Ret: {r.total_return_pct:+6.2f}% | WR: {r.win_rate:5.1f}% | PF: {r.profit_factor:5.2f} | DD: {r.max_drawdown_pct:5.2f}% | N: {r.trade_count:3}")

    # 全体ランキング
    all_results.sort(key=lambda x: x[2].total_return_pct, reverse=True)

    print(f"\n{'='*80}")
    print("★ 全体ランキング TOP 20")
    print(f"{'='*80}")
    print(f"{'Univ':<8} | {'銘柄':<8} | {'戦略':<50} | {'リターン':>8} | {'勝率':>6} | {'PF':>6} | {'MaxDD':>6} | {'N':>4} | {'Sharpe':>7}")
    print("-" * 120)

    for u, s, r in all_results[:20]:
        print(f"{u:<8} | {s:<8} | {r.strategy_name:<50} | {r.total_return_pct:>+7.2f}% | {r.win_rate:>5.1f}% | {r.profit_factor:>5.2f} | {r.max_drawdown_pct:>5.2f}% | {r.trade_count:>4} | {r.sharpe_ratio:>+6.2f}")

    # レポート保存
    save_report(all_results[:20])

    return all_results


def save_report(top_results: list[tuple[str, str, BacktestResult]]):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M")
    path = REPORTS_DIR / f"{now}_optimization_results.md"

    lines = [
        "# バックテスト最適化結果\n",
        f"- 実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"- 初期資金: ${INITIAL_CAPITAL:,.0f}",
        "- データ: yfinance 5分足 60日分",
        "- ルール: ロングのみ / 1銘柄最大$1,100 / EOD強制決済",
        f"- 最低取引数フィルター: 10回以上\n",
        "## TOP 20\n",
        "| # | Universe | 銘柄 | 戦略 | リターン | 勝率 | PF | MaxDD | 取引数 | Sharpe |",
        "|---|----------|------|------|----------|------|-----|-------|--------|--------|",
    ]

    for i, (u, s, r) in enumerate(top_results, 1):
        lines.append(
            f"| {i} | {u} | {s} | {r.strategy_name} | {r.total_return_pct:+.2f}% | {r.win_rate:.1f}% | {r.profit_factor:.2f} | {r.max_drawdown_pct:.2f}% | {r.trade_count} | {r.sharpe_ratio:+.2f} |"
        )

    # 上位戦略の詳細トレード
    if top_results:
        best_u, best_s, best_r = top_results[0]
        lines.append(f"\n## 最優秀: {best_r.strategy_name} on {best_s} ({best_u})\n")
        lines.append(f"- リターン: {best_r.total_return_pct:+.2f}%")
        lines.append(f"- 最終資金: ${best_r.final_capital:,.2f}")
        lines.append(f"- 勝率: {best_r.win_rate:.1f}%")
        lines.append(f"- PF: {best_r.profit_factor:.2f}")
        lines.append(f"- MaxDD: {best_r.max_drawdown_pct:.2f}%")
        lines.append(f"- Sharpe: {best_r.sharpe_ratio:+.2f}\n")
        lines.append("### トレード詳細\n")
        lines.append("| 日時 | 銘柄 | 数量 | Entry | Exit | P&L | 理由 |")
        lines.append("|------|------|------|-------|------|-----|------|")
        for t in best_r.trades[:50]:
            lines.append(f"| {t.entry_time.strftime('%m/%d %H:%M')} | {t.symbol} | {t.qty:.0f} | ${t.entry_price:.2f} | ${t.exit_price:.2f} | ${t.pnl:+.2f} | {t.reason} |")

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nレポート保存: {path}")


if __name__ == "__main__":
    run_scan()
