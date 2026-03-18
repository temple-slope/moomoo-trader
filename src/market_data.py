"""相場データ取得（株価、板情報、ローソク足）"""

import pandas as pd
from moomoo import KLType, RET_OK, SubType

from .client import MoomooClient


def get_quote(client: MoomooClient, code: str) -> pd.DataFrame:
    """リアルタイム株価を取得"""
    ret, data = client.quote_ctx.get_stock_quote([code])
    if ret != RET_OK:
        raise RuntimeError(f"株価取得失敗: {data}")
    return data


def get_kline(
    client: MoomooClient,
    code: str,
    ktype: str = "K_1M",
    count: int = 100,
) -> pd.DataFrame:
    """ローソク足データを取得"""
    kl = getattr(KLType, ktype)
    ret, data, _ = client.quote_ctx.request_history_kline(
        code, ktype=kl, max_count=count
    )
    if ret != RET_OK:
        raise RuntimeError(f"Kline取得失敗: {data}")
    return data


def get_orderbook(client: MoomooClient, code: str) -> pd.DataFrame:
    """板情報を取得"""
    ret, data = client.quote_ctx.get_order_book(code)
    if ret != RET_OK:
        raise RuntimeError(f"板情報取得失敗: {data}")
    return data


def subscribe(client: MoomooClient, codes: list[str], sub_types: list[SubType]) -> None:
    """リアルタイムデータを購読"""
    ret, data = client.quote_ctx.subscribe(codes, sub_types)
    if ret != RET_OK:
        raise RuntimeError(f"購読失敗: {data}")
