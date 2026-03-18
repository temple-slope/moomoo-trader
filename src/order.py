"""注文発注・管理"""

import pandas as pd
from moomoo import OrderType, RET_OK, TrdSide

from .client import MoomooClient


def _parse_side(side: str | TrdSide) -> TrdSide:
    if isinstance(side, TrdSide):
        return side
    s = side.upper()
    if s == "BUY":
        return TrdSide.BUY
    if s == "SELL":
        return TrdSide.SELL
    raise ValueError(f"Invalid side: {side!r} (expected 'BUY' or 'SELL')")


def place_limit_order(
    client: MoomooClient,
    code: str,
    side: str | TrdSide,
    qty: float,
    price: float,
    remark: str = "",
) -> pd.DataFrame:
    """指値注文"""
    trd_side = _parse_side(side)
    ret, data = client.trade_ctx.place_order(
        price=price,
        qty=qty,
        code=code,
        trd_side=trd_side,
        order_type=OrderType.NORMAL,
        trd_env=client.trd_env,
        remark=remark,
    )
    if ret != RET_OK:
        raise RuntimeError(f"指値注文失敗: {data}")
    return data


def place_market_order(
    client: MoomooClient,
    code: str,
    side: str | TrdSide,
    qty: float,
    remark: str = "",
) -> pd.DataFrame:
    """成行注文"""
    trd_side = _parse_side(side)
    ret, data = client.trade_ctx.place_order(
        price=0,
        qty=qty,
        code=code,
        trd_side=trd_side,
        order_type=OrderType.MARKET,
        trd_env=client.trd_env,
        remark=remark,
    )
    if ret != RET_OK:
        raise RuntimeError(f"成行注文失敗: {data}")
    return data


def place_stop_order(
    client: MoomooClient,
    code: str,
    side: str | TrdSide,
    qty: float,
    aux_price: float,
    price: float = 0,
    remark: str = "",
) -> pd.DataFrame:
    """逆指値注文"""
    trd_side = _parse_side(side)
    ret, data = client.trade_ctx.place_order(
        price=price,
        qty=qty,
        code=code,
        trd_side=trd_side,
        order_type=OrderType.STOP,
        aux_price=aux_price,
        trd_env=client.trd_env,
        remark=remark,
    )
    if ret != RET_OK:
        raise RuntimeError(f"逆指値注文失敗: {data}")
    return data


def get_orders(client: MoomooClient) -> pd.DataFrame:
    """注文一覧を取得"""
    ret, data = client.trade_ctx.order_list_query(trd_env=client.trd_env)
    if ret != RET_OK:
        raise RuntimeError(f"注文一覧取得失敗: {data}")
    return data


def cancel_order(client: MoomooClient, order_id: str) -> pd.DataFrame:
    """注文をキャンセル"""
    ret, data = client.trade_ctx.modify_order(
        modify_order_op="CANCEL",
        order_id=order_id,
        qty=0,
        price=0,
        trd_env=client.trd_env,
    )
    if ret != RET_OK:
        raise RuntimeError(f"注文キャンセル失敗: {data}")
    return data
