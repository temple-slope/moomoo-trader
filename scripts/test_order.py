"""シミュレーション環境でのテスト注文スクリプト

使い方:
  python scripts/test_order.py                    # HK.00700 に指値買い注文
  python scripts/test_order.py --code HK.00700 --side BUY --qty 100 --price 300
  python scripts/test_order.py --market           # 成行注文
  python scripts/test_order.py --cancel ORDER_ID  # 注文キャンセル
  python scripts/test_order.py --list             # 注文一覧表示
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from moomoo import RET_OK

from src.client import create_client
from src.order import cancel_order, place_limit_order, place_market_order
from src.portfolio import get_account_info, get_orders, get_positions


def show_account(client) -> None:
    """口座残高とポジションを表示"""
    print("--- 口座情報 (SIMULATE) ---")
    try:
        info = get_account_info(client, currency="HKD")
        cols = [c for c in ["total_assets", "cash", "market_val", "avl_withdrawal_cash"]
                if c in info.columns]
        print(info[cols].to_string(index=False))
    except RuntimeError as e:
        print(f"  {e}")

    print("\n--- ポジション ---")
    try:
        pos = get_positions(client)
        if pos.empty:
            print("  なし")
        else:
            cols = [c for c in ["code", "stock_name", "qty", "cost_price", "market_val", "pl_val"]
                    if c in pos.columns]
            print(pos[cols].to_string(index=False))
    except RuntimeError as e:
        print(f"  {e}")


def show_orders(client) -> None:
    """注文一覧を表示"""
    print("--- 注文一覧 (SIMULATE) ---")
    try:
        orders = get_orders(client)
        if orders.empty:
            print("  注文なし")
        else:
            cols = [c for c in ["order_id", "code", "trd_side", "order_type", "qty", "price",
                                "order_status", "create_time"]
                    if c in orders.columns]
            print(orders[cols].to_string(index=False))
    except RuntimeError as e:
        print(f"  {e}")


def get_current_price(client, code: str) -> float | None:
    """現在値を取得（指値の参考用）"""
    ret, data = client.quote_ctx.get_market_snapshot([code])
    if ret == RET_OK and not data.empty:
        return float(data["last_price"].iloc[0])
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="シミュレーション環境テスト注文")
    parser.add_argument("--code", default="HK.00700", help="銘柄コード (default: HK.00700)")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"], help="売買方向")
    parser.add_argument("--qty", type=float, default=100, help="数量 (default: 100)")
    parser.add_argument("--price", type=float, default=None, help="指値価格 (省略時は現在値-5%%)")
    parser.add_argument("--market", action="store_true", help="成行注文にする")
    parser.add_argument("--cancel", metavar="ORDER_ID", help="指定注文をキャンセル")
    parser.add_argument("--list", action="store_true", help="注文一覧のみ表示")
    args = parser.parse_args()

    print("=== moomoo シミュレーション注文テスト ===\n")

    with create_client(trade_env="SIMULATE") as client:
        # 口座状況を表示
        show_account(client)
        print()

        # --list: 一覧表示のみ
        if args.list:
            show_orders(client)
            return 0

        # --cancel: キャンセル
        if args.cancel:
            print(f"注文キャンセル: {args.cancel}")
            try:
                result = cancel_order(client, args.cancel)
                print("  キャンセル成功")
                print(result.to_string(index=False))
            except RuntimeError as e:
                print(f"  {e}")
            return 0

        # 現在値を取得
        current = get_current_price(client, args.code)
        if current:
            print(f"現在値: {args.code} = {current}")
        else:
            print(f"警告: {args.code} の現在値を取得できませんでした")

        # 注文発注
        if args.market:
            print(f"\n成行注文: {args.side} {args.code} x {args.qty}")
            try:
                result = place_market_order(client, args.code, args.side, args.qty,
                                            remark="test_order_script")
                print("  発注成功:")
                print(result.to_string(index=False))
            except RuntimeError as e:
                print(f"  {e}")
        else:
            price = args.price
            if price is None and current:
                # 買いなら現在値の5%下、売りなら5%上に指値
                if args.side == "BUY":
                    price = round(current * 0.95, 2)
                else:
                    price = round(current * 1.05, 2)
            elif price is None:
                print("エラー: 現在値が取得できないため --price を指定してください")
                return 1

            print(f"\n指値注文: {args.side} {args.code} x {args.qty} @ {price}")
            try:
                result = place_limit_order(client, args.code, args.side, args.qty, price,
                                           remark="test_order_script")
                print("  発注成功:")
                print(result.to_string(index=False))
            except RuntimeError as e:
                print(f"  {e}")

        # 発注後の注文一覧
        print()
        show_orders(client)

    return 0


if __name__ == "__main__":
    sys.exit(main())
