"""ポジション・口座情報照会"""

import pandas as pd
from moomoo import RET_OK

from .client import MoomooClient


def get_positions(client: MoomooClient) -> pd.DataFrame:
    """保有ポジションを取得"""
    ret, data = client.trade_ctx.position_list_query(trd_env=client.trd_env)
    if ret != RET_OK:
        raise RuntimeError(f"ポジション取得失敗: {data}")
    return data


def get_account_info(client: MoomooClient, currency: str = "JPY") -> pd.DataFrame:
    """口座情報を取得"""
    ret, data = client.trade_ctx.accinfo_query(trd_env=client.trd_env, currency=currency)
    if ret != RET_OK:
        raise RuntimeError(f"口座情報取得失敗: {data}")
    return data


def get_orders(client: MoomooClient) -> pd.DataFrame:
    """注文一覧を取得"""
    ret, data = client.trade_ctx.order_list_query(trd_env=client.trd_env)
    if ret != RET_OK:
        raise RuntimeError(f"注文一覧取得失敗: {data}")
    return data


def get_deal_list(client: MoomooClient) -> pd.DataFrame:
    """約定履歴を取得"""
    ret, data = client.trade_ctx.deal_list_query(trd_env=client.trd_env)
    if ret != RET_OK:
        raise RuntimeError(f"約定履歴取得失敗: {data}")
    return data
