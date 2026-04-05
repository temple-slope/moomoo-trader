"""バックテストエンジン。日ごとにループし、戦略シグナルに基づきトレードをシミュレーション。"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from .strategies import Strategy, Signal
from .data import split_by_day, calc_daily_atr


@dataclass
class Trade:
    symbol: str
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    reason: str


@dataclass
class BacktestResult:
    strategy_name: str
    trades: list[Trade] = field(default_factory=list)
    initial_capital: float = 3300.0
    final_capital: float = 3300.0

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
        return gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

    @property
    def max_drawdown_pct(self) -> float:
        if not self.trades:
            return 0.0
        equity = [self.initial_capital]
        for t in self.trades:
            equity.append(equity[-1] + t.pnl)
        peak = equity[0]
        max_dd = 0.0
        for e in equity:
            peak = max(peak, e)
            dd = (peak - e) / peak * 100
            max_dd = max(max_dd, dd)
        return max_dd

    @property
    def sharpe_ratio(self) -> float:
        if len(self.trades) < 2:
            return 0.0
        returns = [t.pnl / self.initial_capital for t in self.trades]
        mean_r = np.mean(returns)
        std_r = np.std(returns)
        if std_r == 0:
            return 0.0
        # 年率換算（252営業日）
        return float(mean_r / std_r * np.sqrt(252))


CLOSING_HOUR = 15
CLOSING_MINUTE = 45
MAX_POSITION_PER_SYMBOL = 1100.0  # $1,100


def run_backtest(
    strategy: Strategy,
    data: dict[str, pd.DataFrame],
    initial_capital: float = 3300.0,
    symbols: list[str] | None = None,
) -> BacktestResult:
    """1つの戦略を全銘柄・全日に対してバックテストする。"""
    result = BacktestResult(strategy_name=strategy.name, initial_capital=initial_capital)
    capital = initial_capital
    symbols = symbols or list(data.keys())

    for sym in symbols:
        df = data.get(sym)
        if df is None or df.empty:
            continue

        days = split_by_day(df)
        for day_df in days:
            if len(day_df) < 20:
                continue

            signals = strategy.generate_signals(day_df)
            position = None  # (entry_signal, qty)

            for sig in signals:
                if sig.action == "BUY" and position is None:
                    alloc = min(MAX_POSITION_PER_SYMBOL, capital)
                    if alloc < 10:
                        continue
                    qty = int(alloc / sig.price)
                    if qty < 1:
                        continue
                    cost = qty * sig.price
                    capital -= cost
                    position = (sig, qty)

                elif sig.action == "SELL" and position is not None:
                    entry_sig, qty = position
                    proceeds = qty * sig.price
                    pnl = proceeds - qty * entry_sig.price
                    capital += proceeds
                    result.trades.append(Trade(
                        symbol=sym,
                        entry_time=entry_sig.timestamp,
                        exit_time=sig.timestamp,
                        entry_price=entry_sig.price,
                        exit_price=sig.price,
                        qty=qty,
                        pnl=pnl,
                        reason=sig.reason,
                    ))
                    position = None

            # 時間切れ: 引け前に強制決済
            if position is not None:
                entry_sig, qty = position
                last_bar = day_df.iloc[-1]
                exit_price = last_bar["close"]
                pnl = qty * exit_price - qty * entry_sig.price
                capital += qty * exit_price
                result.trades.append(Trade(
                    symbol=sym,
                    entry_time=entry_sig.timestamp,
                    exit_time=day_df.index[-1],
                    entry_price=entry_sig.price,
                    exit_price=exit_price,
                    qty=qty,
                    pnl=pnl,
                    reason="EOD強制決済",
                ))

    result.final_capital = capital
    return result
