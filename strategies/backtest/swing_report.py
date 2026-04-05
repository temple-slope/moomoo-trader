"""Dual Momentum / Breakout Swing のバックテスト実行 + レポート生成。"""

from datetime import datetime
from pathlib import Path
import numpy as np

from .data import fetch_daily, fetch_daily_combined, SECTOR_ETFS, BOND_ETF, SWING_UNIVERSE, JP_SECTOR_ETFS
from .strategies import DualMomentum, BreakoutSwing, LeadLagPCA, SWING_STRATEGIES
from .swing_runner import run_swing_backtest, SwingBacktestResult

INITIAL_CAPITAL = 30000.0
REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "reports" / "backtest"


def format_results(results: list[SwingBacktestResult]) -> str:
    header = f"| {'戦略':<45} | {'総リターン':>10} | {'勝率':>8} | {'PF':>6} | {'最大DD':>8} | {'取引数':>6} | {'Sharpe':>7} | {'平均保有':>8} |"
    sep = f"|{'-' * 47}|{'-' * 12}|{'-' * 10}|{'-' * 8}|{'-' * 10}|{'-' * 8}|{'-' * 9}|{'-' * 10}|"
    rows = []
    for r in results:
        rows.append(
            f"| {r.strategy_name:<45} | {r.total_return_pct:>+9.2f}% | {r.win_rate:>7.1f}% | {r.profit_factor:>6.2f} | {r.max_drawdown_pct:>7.2f}% | {r.trade_count:>6} | {r.sharpe_ratio:>+7.2f} | {r.avg_hold_days:>6.1f}日 |"
        )
    return "\n".join([header, sep] + rows)


def generate_report(results: list[SwingBacktestResult]) -> str:
    valid = [r for r in results if not np.isnan(r.total_return_pct)]
    if not valid:
        valid = results
    best = max(valid, key=lambda r: r.total_return_pct)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    report = f"""# 米株スイング戦略 バックテスト結果

- 実行日時: {now}
- 初期資金: ${INITIAL_CAPITAL:,.0f}
- データ: yfinance 日足 60日分
- Dual Momentum: セクターETF ({', '.join(SECTOR_ETFS)}) + {BOND_ETF}
- Breakout Swing: S&P500高流動性 {len(SWING_UNIVERSE)}銘柄

## 戦略比較

{format_results(results)}

## 最優秀戦略: {best.strategy_name}

- 総リターン: {best.total_return_pct:+.2f}%
- 最終資金: ${best.final_capital:,.2f}
- 取引数: {best.trade_count}
- 勝率: {best.win_rate:.1f}%
- プロフィットファクター: {best.profit_factor:.2f}
- 最大ドローダウン: {best.max_drawdown_pct:.2f}%
- シャープレシオ: {best.sharpe_ratio:+.2f}
- 平均保有日数: {best.avg_hold_days:.1f}日

## トレード詳細

| 日時 | 銘柄 | 数量 | Entry | Exit | P&L | 保有日数 | 理由 |
|------|------|------|-------|------|-----|----------|------|
"""
    for t in best.trades:
        hold = (t.exit_date - t.entry_date).days if t.exit_date else 0
        report += f"| {t.entry_date.strftime('%m/%d')} | {t.symbol} | {t.qty:.0f} | ${t.entry_price:.2f} | ${t.exit_price:.2f} | ${t.pnl:+.2f} | {hold}日 | {t.reason} |\n"

    return report


def main():
    print("=== 米株スイング戦略 バックテスト ===\n")

    # Dual Momentum 用データ取得
    dm_symbols = SECTOR_ETFS + [BOND_ETF]
    print(f"Dual Momentum データ取得中... ({len(dm_symbols)} ETFs)")
    dm_data = fetch_daily(dm_symbols, period="60d")
    print(f"  取得: {list(dm_data.keys())}")
    for sym, df in dm_data.items():
        print(f"  {sym}: {len(df)}日 ({df.index.min().strftime('%Y-%m-%d')} ~ {df.index.max().strftime('%Y-%m-%d')})")

    # Breakout Swing 用データ取得
    print(f"\nBreakout Swing データ取得中... ({len(SWING_UNIVERSE)} 銘柄)")
    sw_data = fetch_daily(SWING_UNIVERSE, period="60d")
    print(f"  取得: {len(sw_data)} 銘柄")

    print("\nバックテスト実行中...\n")
    results = []

    # Dual Momentum
    dm_strat = DualMomentum(lookback=40, top_n=3)
    dm_result = run_swing_backtest(dm_strat, dm_data, initial_capital=INITIAL_CAPITAL)
    results.append(dm_result)
    print(f"  {dm_strat.name:<45} -> リターン: {dm_result.total_return_pct:+.2f}%, 取引: {dm_result.trade_count}回")

    # Dual Momentum バリアント
    for lb, n in [(20, 2), (40, 4), (30, 3)]:
        strat = DualMomentum(lookback=lb, top_n=n)
        r = run_swing_backtest(strat, dm_data, initial_capital=INITIAL_CAPITAL)
        results.append(r)
        print(f"  {strat.name:<45} -> リターン: {r.total_return_pct:+.2f}%, 取引: {r.trade_count}回")

    # Breakout Swing
    bs_strat = BreakoutSwing(breakout_period=20, vol_mult=1.5, trail_atr=2.0)
    bs_result = run_swing_backtest(bs_strat, sw_data, initial_capital=INITIAL_CAPITAL)
    results.append(bs_result)
    print(f"  {bs_strat.name:<45} -> リターン: {bs_result.total_return_pct:+.2f}%, 取引: {bs_result.trade_count}回")

    # Breakout Swing バリアント
    for bp, vm, ta in [(15, 1.2, 1.5), (20, 2.0, 2.5), (10, 1.0, 2.0)]:
        strat = BreakoutSwing(breakout_period=bp, vol_mult=vm, trail_atr=ta)
        r = run_swing_backtest(strat, sw_data, initial_capital=INITIAL_CAPITAL)
        results.append(r)
        print(f"  {strat.name:<45} -> リターン: {r.total_return_pct:+.2f}%, 取引: {r.trade_count}回")

    # Lead-Lag PCA (日米業種リードラグ)
    ll_symbols = SECTOR_ETFS + JP_SECTOR_ETFS
    print(f"\nLeadLagPCA データ取得中... (US {len(SECTOR_ETFS)} + JP {len(JP_SECTOR_ETFS)} ETFs)")
    ll_data = fetch_daily(ll_symbols, period="1y")
    print(f"  取得: {len(ll_data)} 銘柄")

    ll_strat = LeadLagPCA()
    ll_result = run_swing_backtest(ll_strat, ll_data, initial_capital=INITIAL_CAPITAL)
    results.append(ll_result)
    print(f"  {ll_strat.name:<45} -> リターン: {ll_result.total_return_pct:+.2f}%, 取引: {ll_result.trade_count}回")

    print("\n" + format_results(results))

    # レポート保存
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{datetime.now().strftime('%Y%m%d')}_swing_backtest.md"
    valid = [r for r in results if not np.isnan(r.total_return_pct)]
    report_content = generate_report(valid if valid else results)
    report_path.write_text(report_content, encoding="utf-8")
    print(f"\nレポート保存: {report_path}")

    if valid:
        best = max(valid, key=lambda r: r.total_return_pct)
        print(f"\n★ 最優秀戦略: {best.strategy_name} ({best.total_return_pct:+.2f}%)")


if __name__ == "__main__":
    main()
