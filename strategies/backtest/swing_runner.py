"""スイング/ポジション戦略用バックテストエンジン。日足ベースで複数日保有をシミュレーション。"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from .strategies import SwingStrategy, SwingSignal


@dataclass
class SwingTrade:
    symbol: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp | None
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    reason: str


@dataclass
class SwingBacktestResult:
    strategy_name: str
    trades: list[SwingTrade] = field(default_factory=list)
    initial_capital: float = 30000.0
    final_capital: float = 30000.0
    equity_curve: list[tuple[pd.Timestamp, float]] = field(default_factory=list)

    @property
    def total_return_pct(self) -> float:
        return (self.final_capital - self.initial_capital) / self.initial_capital * 100

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.pnl > 0)
        return wins / len(self.trades) * 100

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        if gross_loss == 0:
            return float("inf") if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    @property
    def max_drawdown_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        values = [e for _, e in self.equity_curve]
        peak = values[0]
        max_dd = 0.0
        for v in values:
            peak = max(peak, v)
            dd = (peak - v) / peak * 100
            max_dd = max(max_dd, dd)
        return max_dd

    @property
    def sharpe_ratio(self) -> float:
        if len(self.equity_curve) < 3:
            return 0.0
        values = [e for _, e in self.equity_curve]
        returns = pd.Series(values).pct_change().dropna()
        if returns.std() == 0:
            return 0.0
        return float(returns.mean() / returns.std() * np.sqrt(252))

    @property
    def avg_hold_days(self) -> float:
        if not self.trades:
            return 0.0
        days = []
        for t in self.trades:
            if t.exit_date is not None:
                delta = (t.exit_date - t.entry_date).days
                days.append(max(delta, 1))
        return sum(days) / len(days) if days else 0.0


def run_swing_backtest(
    strategy: SwingStrategy,
    daily_data: dict[str, pd.DataFrame],
    initial_capital: float = 30000.0,
) -> SwingBacktestResult:
    """スイング戦略のバックテストを実行。"""
    signals = strategy.generate_signals(daily_data)
    if not signals:
        result = SwingBacktestResult(strategy_name=strategy.name, initial_capital=initial_capital)
        result.final_capital = initial_capital
        return result

    # リバランス戦略は専用処理
    if any(s.action == "REBALANCE" for s in signals):
        return _run_rebalance_backtest(strategy, daily_data, initial_capital)

    result = SwingBacktestResult(
        strategy_name=strategy.name,
        initial_capital=initial_capital,
    )

    capital = initial_capital
    positions: dict[str, dict] = {}  # sym -> {qty, entry_price, entry_date}

    signals.sort(key=lambda s: s.date)

    for sig in signals:
        if sig.action == "BUY":
            if sig.symbol in positions:
                continue
            if pd.isna(sig.price) or sig.price <= 0:
                continue
            alloc = capital * sig.weight
            if alloc < 10:
                continue
            qty = int(alloc / sig.price)
            if qty < 1:
                continue
            cost = qty * sig.price
            capital -= cost
            positions[sig.symbol] = {
                "qty": qty, "entry_price": sig.price, "entry_date": sig.date,
            }
        elif sig.action == "SELL":
            pos = positions.pop(sig.symbol, None)
            if pos is None:
                continue
            sell_price = float(sig.price) if pd.notna(sig.price) and sig.price > 0 else pos["entry_price"]
            proceeds = pos["qty"] * sell_price
            pnl = proceeds - pos["qty"] * pos["entry_price"]
            capital += proceeds
            result.trades.append(SwingTrade(
                symbol=sig.symbol, entry_date=pos["entry_date"],
                exit_date=sig.date, entry_price=pos["entry_price"],
                exit_price=sell_price, qty=pos["qty"], pnl=pnl,
                reason=sig.reason,
            ))

    # 未決済ポジションを最終日の終値で評価
    for sym, pos in positions.items():
        df = daily_data.get(sym)
        if df is not None and not df.empty:
            last_price = df["close"].dropna().iloc[-1] if not df["close"].dropna().empty else pos["entry_price"]
            last_price = float(last_price)
            pnl = pos["qty"] * last_price - pos["qty"] * pos["entry_price"]
            capital += pos["qty"] * last_price
            result.trades.append(SwingTrade(
                symbol=sym, entry_date=pos["entry_date"],
                exit_date=df.index[-1], entry_price=pos["entry_price"],
                exit_price=last_price, qty=pos["qty"], pnl=pnl,
                reason="期末評価決済",
            ))

    result.final_capital = capital

    # エクイティカーブ生成
    result.equity_curve = _build_equity_curve(result, daily_data, initial_capital)

    return result


def _run_rebalance_backtest(
    strategy: SwingStrategy,
    daily_data: dict[str, pd.DataFrame],
    initial_capital: float,
) -> SwingBacktestResult:
    """REBALANCE シグナル専用のバックテスト。月次リバランス。"""
    result = SwingBacktestResult(
        strategy_name=strategy.name,
        initial_capital=initial_capital,
    )
    signals = strategy.generate_signals(daily_data)
    signals.sort(key=lambda s: s.date)

    # 日付ごとにグループ化
    from itertools import groupby
    grouped = {k: list(v) for k, v in groupby(signals, key=lambda s: s.date)}

    capital = initial_capital
    positions: dict[str, dict] = {}  # sym -> {qty, entry_price, entry_date}
    equity_curve: list[tuple[pd.Timestamp, float]] = []

    # 全日付を集めてエクイティカーブ用に使う
    all_dates = set()
    for df in daily_data.values():
        all_dates.update(df.index)
    all_dates = sorted(all_dates)

    rebalance_set = set(grouped.keys())
    last_rebalance_done = False

    for date in all_dates:
        if date in rebalance_set:
            sigs = grouped[date]
            # 既存ポジションを全クローズ
            for sym in list(positions.keys()):
                pos = positions.pop(sym)
                df = daily_data.get(sym)
                exit_price = pos["entry_price"]
                if df is not None and date in df.index:
                    p = df["close"].loc[date]
                    if pd.notna(p):
                        exit_price = float(p)
                proceeds = pos["qty"] * exit_price
                pnl = proceeds - pos["qty"] * pos["entry_price"]
                capital += proceeds
                result.trades.append(SwingTrade(
                    symbol=sym, entry_date=pos["entry_date"],
                    exit_date=date, entry_price=pos["entry_price"],
                    exit_price=exit_price, qty=pos["qty"], pnl=pnl,
                    reason="リバランス売却",
                ))

            # 新規ポジション構築
            for sig in sigs:
                if pd.isna(sig.price) or sig.price <= 0:
                    continue
                alloc = capital * abs(sig.weight)
                if alloc < 10:
                    continue
                qty = int(alloc / sig.price)
                if qty < 1:
                    continue
                if sig.weight >= 0:
                    cost = qty * sig.price
                    capital -= cost
                    positions[sig.symbol] = {
                        "qty": qty, "entry_price": sig.price, "entry_date": date,
                    }
                else:
                    # 空売り: 売却代金を受け取り、qty を負数で保持
                    proceeds = qty * sig.price
                    capital += proceeds
                    positions[sig.symbol] = {
                        "qty": -qty, "entry_price": sig.price, "entry_date": date,
                    }

        # エクイティ記録 (毎日)
        total = capital
        for sym, pos in positions.items():
            df = daily_data.get(sym)
            if df is not None and date in df.index:
                p = df["close"].loc[date]
                if pd.notna(p):
                    total += pos["qty"] * float(p)
                else:
                    total += pos["qty"] * pos["entry_price"]
            else:
                total += pos["qty"] * pos["entry_price"]
        equity_curve.append((date, total))

    # 未決済を最終評価
    for sym, pos in positions.items():
        df = daily_data.get(sym)
        if df is not None and not df.empty:
            last_price = df["close"].iloc[-1]
            pnl = pos["qty"] * last_price - pos["qty"] * pos["entry_price"]
            capital += pos["qty"] * last_price
            result.trades.append(SwingTrade(
                symbol=sym, entry_date=pos["entry_date"],
                exit_date=df.index[-1], entry_price=pos["entry_price"],
                exit_price=last_price, qty=pos["qty"], pnl=pnl,
                reason="期末評価決済",
            ))

    result.final_capital = capital
    result.equity_curve = equity_curve
    return result


def _build_equity_curve(
    result: SwingBacktestResult,
    daily_data: dict[str, pd.DataFrame],
    initial_capital: float,
) -> list[tuple[pd.Timestamp, float]]:
    """トレード履歴からエクイティカーブを構築。"""
    all_dates = set()
    for df in daily_data.values():
        all_dates.update(df.index)
    all_dates = sorted(all_dates)

    curve = []
    equity = initial_capital
    active_pnl = 0.0
    closed_pnl = 0.0

    trade_entries = {}
    trade_exits = {}
    for t in result.trades:
        trade_entries.setdefault(t.entry_date, []).append(t)
        if t.exit_date is not None:
            trade_exits.setdefault(t.exit_date, []).append(t)

    holding: dict[str, SwingTrade] = {}

    for date in all_dates:
        # エントリー
        for t in trade_entries.get(date, []):
            holding[t.symbol] = t

        # エグジット
        for t in trade_exits.get(date, []):
            holding.pop(t.symbol, None)
            closed_pnl += t.pnl

        # 未実現P&L
        unrealized = 0.0
        for sym, t in holding.items():
            df = daily_data.get(sym)
            if df is not None and date in df.index:
                cur_price = df["close"].loc[date]
                unrealized += t.qty * (cur_price - t.entry_price)

        curve.append((date, initial_capital + closed_pnl + unrealized))

    return curve
