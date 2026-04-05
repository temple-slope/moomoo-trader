"""バックテスト結果の比較レポートを生成する。"""

import sys
import os
from datetime import datetime
from pathlib import Path

from .data import fetch_intraday, select_top_volatile
from .strategies import ALL_STRATEGIES
from .runner import run_backtest, BacktestResult

INITIAL_CAPITAL = 3300.0
REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "reports" / "backtest"


def format_results_table(results: list[BacktestResult]) -> str:
    """結果を比較テーブルとして整形する。"""
    header = f"| {'戦略':<28} | {'総リターン':>10} | {'勝率':>8} | {'PF':>6} | {'最大DD':>8} | {'取引数':>6} | {'Sharpe':>7} |"
    sep = f"|{'-' * 30}|{'-' * 12}|{'-' * 10}|{'-' * 8}|{'-' * 10}|{'-' * 8}|{'-' * 9}|"

    rows = []
    for r in results:
        rows.append(
            f"| {r.strategy_name:<28} | {r.total_return_pct:>+9.2f}% | {r.win_rate:>7.1f}% | {r.profit_factor:>6.2f} | {r.max_drawdown_pct:>7.2f}% | {r.trade_count:>6} | {r.sharpe_ratio:>+7.2f} |"
        )

    return "\n".join([header, sep] + rows)


def generate_report(results: list[BacktestResult], symbols_used: list[str]) -> str:
    """Markdownレポートを生成する。"""
    best = max(results, key=lambda r: r.total_return_pct)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    report = f"""# バックテスト結果レポート

- 実行日時: {now}
- 対象銘柄: {', '.join(symbols_used)}
- 初期資金: ${INITIAL_CAPITAL:,.0f}
- データ: yfinance 5分足 60日分
- ルール: ロングのみ / 1銘柄最大$1,100 / EOD強制決済

## 戦略比較

{format_results_table(results)}

## 最優秀戦略: {best.strategy_name}

- 総リターン: {best.total_return_pct:+.2f}%
- 最終資金: ${best.final_capital:,.2f}
- 取引数: {best.trade_count}
- 勝率: {best.win_rate:.1f}%
- プロフィットファクター: {best.profit_factor:.2f}
- 最大ドローダウン: {best.max_drawdown_pct:.2f}%
- シャープレシオ: {best.sharpe_ratio:+.2f}

## トレード詳細 (上位戦略)

| 日時 | 銘柄 | 方向 | 数量 | エントリー | エグジット | P&L | 理由 |
|------|------|------|------|-----------|-----------|-----|------|
"""
    for t in best.trades[:30]:
        report += f"| {t.entry_time.strftime('%m/%d %H:%M')} | {t.symbol} | LONG | {t.qty:.0f} | ${t.entry_price:.2f} | ${t.exit_price:.2f} | ${t.pnl:+.2f} | {t.reason} |\n"

    if best.trade_count > 30:
        report += f"\n*({best.trade_count - 30}件省略)*\n"

    return report


def main():
    print("=== 米国株デイトレ バックテスト ===\n")

    print("5分足データ取得中...")
    data = fetch_intraday()
    print(f"取得完了: {list(data.keys())}")

    for sym, df in data.items():
        print(f"  {sym}: {len(df)}本 ({df.index.min().strftime('%Y-%m-%d')} ~ {df.index.max().strftime('%Y-%m-%d')})")

    symbols = select_top_volatile(data, n=3)
    if not symbols:
        symbols = list(data.keys())[:3]
    print(f"\n選定銘柄 (ボラティリティ上位3): {symbols}")

    # 選定銘柄のデータのみ使用
    selected_data = {s: data[s] for s in symbols if s in data}

    print("\nバックテスト実行中...\n")
    results = []
    for strategy in ALL_STRATEGIES:
        result = run_backtest(strategy, selected_data, initial_capital=INITIAL_CAPITAL)
        results.append(result)
        print(f"  {strategy.name:<28} → リターン: {result.total_return_pct:+.2f}%, 取引: {result.trade_count}回")

    print("\n" + format_results_table(results))

    # レポート保存
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{datetime.now().strftime('%Y%m%d')}_backtest_results.md"
    report_content = generate_report(results, symbols)
    report_path.write_text(report_content, encoding="utf-8")
    print(f"\nレポート保存: {report_path}")

    best = max(results, key=lambda r: r.total_return_pct)
    print(f"\n★ 最優秀戦略: {best.strategy_name} ({best.total_return_pct:+.2f}%)")


if __name__ == "__main__":
    main()
