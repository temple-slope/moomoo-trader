"""サンプル戦略: 移動平均クロスオーバー"""

import time

import pandas as pd

from src.client import close_client, create_client
from src.market_data import get_kline
from src.order import place_market_order
from src.portfolio import get_positions


def calc_ma(df: pd.DataFrame, period: int) -> pd.Series:
    return df["close"].rolling(window=period).mean()


def run(code: str = "US.AAPL", short_period: int = 5, long_period: int = 20):
    client = create_client()

    try:
        while True:
            kline = get_kline(client, code, ktype="K_1M", count=long_period + 10)
            ma_short = calc_ma(kline, short_period)
            ma_long = calc_ma(kline, long_period)

            if pd.isna(ma_short.iloc[-1]) or pd.isna(ma_long.iloc[-1]):
                time.sleep(60)
                continue

            positions = get_positions(client)
            has_position = not positions.empty and code in positions["code"].values

            # ゴールデンクロス: 買い
            if ma_short.iloc[-1] > ma_long.iloc[-1] and not has_position:
                print(f"BUY signal: {code}")
                place_market_order(client, code, "BUY", qty=1, remark="ma_cross")

            # デッドクロス: 売り
            elif ma_short.iloc[-1] < ma_long.iloc[-1] and has_position:
                print(f"SELL signal: {code}")
                place_market_order(client, code, "SELL", qty=1, remark="ma_cross")

            time.sleep(60)
    except KeyboardInterrupt:
        print("停止")
    finally:
        close_client(client)


if __name__ == "__main__":
    run()
