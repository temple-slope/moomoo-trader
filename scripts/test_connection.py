"""OpenD疎通テスト: HK.00700のスナップショットを取得"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from moomoo import RET_OK

from src.client import close_client, create_client


def main() -> int:
    print("OpenDへ接続中...")
    client = create_client()

    try:
        code = "HK.00700"
        ret, data = client.quote_ctx.get_market_snapshot([code])

        if ret != RET_OK:
            print(f"スナップショット取得失敗: {data}")
            return 1

        print(f"\n=== {code} スナップショット ===")
        cols = ["code", "name", "last_price", "open_price", "high_price", "low_price", "volume"]
        available = [c for c in cols if c in data.columns]
        print(data[available].to_string(index=False))
        print("\n疎通テスト成功")
        return 0

    except Exception as e:
        print(f"エラー: {e}")
        return 1

    finally:
        close_client(client)


if __name__ == "__main__":
    sys.exit(main())
