"""口座残高・ポジション取得テスト"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from moomoo import OpenSecTradeContext, TrdEnv, RET_OK, SecurityFirm


def main() -> int:
    print("=== moomoo証券 (FUTUJP / JP / REAL) 口座情報取得 ===\n")

    ctx = OpenSecTradeContext(
        host="127.0.0.1", port=11111,
        security_firm=SecurityFirm.FUTUJP, filter_trdmarket="JP",
    )

    try:
        # 口座情報（JPY通貨）
        print("--- 口座情報 ---")
        ret, data = ctx.accinfo_query(trd_env=TrdEnv.REAL, currency="JPY")
        if ret == RET_OK:
            print(data.to_string(index=False))
        else:
            print(f"失敗: {data}")

        # ポジション
        print("\n--- ポジション ---")
        ret, data = ctx.position_list_query(trd_env=TrdEnv.REAL)
        if ret == RET_OK:
            if data.empty:
                print("保有ポジションなし")
            else:
                print(data.to_string(index=False))
        else:
            print(f"失敗: {data}")

        # 注文一覧
        print("\n--- 注文一覧 ---")
        ret, data = ctx.order_list_query(trd_env=TrdEnv.REAL)
        if ret == RET_OK:
            if data.empty:
                print("注文なし")
            else:
                print(data.to_string(index=False))
        else:
            print(f"失敗: {data}")

        return 0
    except Exception as e:
        print(f"エラー: {e}")
        return 1
    finally:
        ctx.close()


if __name__ == "__main__":
    sys.exit(main())
