"""moomoo OpenD ブローカー実装 - 既存 src/ 関数群をラップ"""

from __future__ import annotations

from typing import Any

from moomoo import SubType

from ..client import MoomooClient, create_client
from ..market_data import get_kline, get_orderbook, get_quote, subscribe
from ..order import cancel_order as _cancel_order
from ..order import place_limit_order as _place_limit
from ..order import place_market_order as _place_market
from ..portfolio import get_account_info as _get_account
from ..portfolio import get_deal_list, get_orders as _get_orders
from ..portfolio import get_positions as _get_positions
from .base import AccountInfo, OrderResult, Position


class MomooBroker:
    """moomoo APIを使用するブローカー"""

    def __init__(self, client: MoomooClient | None = None, **kwargs: Any) -> None:
        self._client = client or create_client(**kwargs)
        self._owns_client = client is None

    @property
    def name(self) -> str:
        return "moomoo"

    @property
    def client(self) -> MoomooClient:
        return self._client

    def place_limit_order(
        self, code: str, side: str, qty: float, price: float, remark: str = ""
    ) -> OrderResult:
        df = _place_limit(self._client, code, side, qty, price, remark)
        row = df.to_dict("records")[0] if not df.empty else {}
        return OrderResult(
            order_id=str(row.get("order_id", "")),
            code=code,
            side=side.upper(),
            qty=qty,
            price=price,
            status="submitted",
            raw=row,
        )

    def place_market_order(
        self, code: str, side: str, qty: float, remark: str = ""
    ) -> OrderResult:
        df = _place_market(self._client, code, side, qty, remark)
        row = df.to_dict("records")[0] if not df.empty else {}
        return OrderResult(
            order_id=str(row.get("order_id", "")),
            code=code,
            side=side.upper(),
            qty=qty,
            price=0,
            status="submitted",
            raw=row,
        )

    def cancel_order(self, order_id: str) -> OrderResult:
        df = _cancel_order(self._client, order_id)
        row = df.to_dict("records")[0] if not df.empty else {}
        return OrderResult(
            order_id=order_id,
            code=row.get("code", ""),
            side="",
            qty=0,
            price=0,
            status="cancelled",
            raw=row,
        )

    def get_positions(self) -> list[Position]:
        df = _get_positions(self._client)
        if df.empty:
            return []
        return [
            Position(
                code=row.get("code", ""),
                name=row.get("stock_name", ""),
                qty=row.get("qty", 0),
                cost_price=row.get("cost_price", 0),
                market_value=row.get("market_val", 0),
                unrealized_pnl=row.get("pl_val", 0),
                raw=row,
            )
            for row in df.to_dict("records")
        ]

    def get_account_info(self) -> AccountInfo:
        df = _get_account(self._client)
        row = df.to_dict("records")[0] if not df.empty else {}
        return AccountInfo(
            total_assets=row.get("total_assets", 0),
            cash=row.get("cash", 0),
            market_value=row.get("market_val", 0),
            unrealized_pnl=row.get("unrealized_pl", 0),
            buying_power=row.get("avl_withdrawal_cash", 0),
            raw=row,
        )

    def get_orders(self) -> list[dict[str, Any]]:
        df = _get_orders(self._client)
        return df.to_dict("records") if not df.empty else []

    def get_deals(self) -> list[dict[str, Any]]:
        df = get_deal_list(self._client)
        return df.to_dict("records") if not df.empty else []

    def get_quote(self, code: str) -> dict[str, Any]:
        subscribe(self._client, [code], [SubType.QUOTE])
        df = get_quote(self._client, code)
        return df.to_dict("records")[0] if not df.empty else {}

    def get_kline(self, code: str, ktype: str = "K_1M", count: int = 100) -> list[dict[str, Any]]:
        df = get_kline(self._client, code, ktype=ktype, count=count)
        return df.to_dict("records") if not df.empty else []

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
