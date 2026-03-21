"""kabuステーション REST API ブローカー実装"""

from __future__ import annotations

import logging
from typing import Any

from shared.http_client import api_get, api_post, create_http_client

from .base import AccountInfo, OrderResult, Position

logger = logging.getLogger(__name__)

# kabuステーションの売買区分
SIDE_MAP = {"BUY": "2", "SELL": "1"}
SIDE_REVERSE = {"1": "SELL", "2": "BUY"}

# 注文種別
CASH_MARGIN_TYPE = 1  # 現物
ACCOUNT_TYPE = 2  # 特定口座
FRONT_ORDER_TYPE_LIMIT = 20  # 指値
FRONT_ORDER_TYPE_MARKET = 10  # 成行


class KabuBroker:
    """kabuステーション REST API を使用するブローカー"""

    def __init__(
        self,
        api_password: str,
        host: str = "localhost",
        port: int = 18080,
    ) -> None:
        if not api_password:
            raise ValueError("KABU_API_PASSWORD が未設定です")
        self._base_url = f"http://{host}:{port}/kabusapi"
        self._client = create_http_client(base_url=self._base_url)
        self._token = self._authenticate(api_password)
        self._client.headers["X-API-KEY"] = self._token

    def _authenticate(self, password: str) -> str:
        """APIトークンを取得"""
        data = api_post(self._client, "/token", json_data={"APIPassword": password})
        token = data.get("Token", "")
        if not token:
            raise RuntimeError("kabuステーション認証失敗")
        logger.info("kabuステーション認証成功")
        return token

    @property
    def name(self) -> str:
        return "kabu"

    def place_limit_order(
        self, code: str, side: str, qty: float, price: float, remark: str = ""
    ) -> OrderResult:
        payload = {
            "Symbol": code,
            "Exchange": 1,  # 東証
            "SecurityType": 1,  # 株式
            "Side": SIDE_MAP[side.upper()],
            "CashMargin": CASH_MARGIN_TYPE,
            "DelivType": 2,  # 預り金
            "AccountType": ACCOUNT_TYPE,
            "Qty": int(qty),
            "FrontOrderType": FRONT_ORDER_TYPE_LIMIT,
            "Price": price,
        }
        data = api_post(self._client, "/sendorder", json_data=payload)
        return OrderResult(
            order_id=str(data.get("OrderId", "")),
            code=code,
            side=side.upper(),
            qty=qty,
            price=price,
            status="submitted" if data.get("Result") == 0 else "failed",
            message=str(data.get("Message", "")),
            raw=data,
        )

    def place_market_order(
        self, code: str, side: str, qty: float, remark: str = ""
    ) -> OrderResult:
        payload = {
            "Symbol": code,
            "Exchange": 1,
            "SecurityType": 1,
            "Side": SIDE_MAP[side.upper()],
            "CashMargin": CASH_MARGIN_TYPE,
            "DelivType": 2,
            "AccountType": ACCOUNT_TYPE,
            "Qty": int(qty),
            "FrontOrderType": FRONT_ORDER_TYPE_MARKET,
            "Price": 0,
        }
        data = api_post(self._client, "/sendorder", json_data=payload)
        return OrderResult(
            order_id=str(data.get("OrderId", "")),
            code=code,
            side=side.upper(),
            qty=qty,
            price=0,
            status="submitted" if data.get("Result") == 0 else "failed",
            message=str(data.get("Message", "")),
            raw=data,
        )

    def cancel_order(self, order_id: str) -> OrderResult:
        payload = {"OrderId": order_id}
        data = api_post(self._client, "/cancelorder", json_data=payload)
        return OrderResult(
            order_id=order_id,
            code="",
            side="",
            qty=0,
            price=0,
            status="cancelled" if data.get("Result") == 0 else "failed",
            message=str(data.get("Message", "")),
            raw=data,
        )

    def get_positions(self) -> list[Position]:
        data = api_get(self._client, "/positions")
        if not isinstance(data, list):
            return []
        return [
            Position(
                code=p.get("Symbol", ""),
                name=p.get("SymbolName", ""),
                qty=p.get("LeavesQty", 0),
                cost_price=p.get("Price", 0),
                market_value=p.get("CurrentPrice", 0) * p.get("LeavesQty", 0),
                unrealized_pnl=p.get("ProfitLoss", 0),
                side=SIDE_REVERSE.get(str(p.get("Side", "")), "LONG"),
                raw=p,
            )
            for p in data
        ]

    def get_account_info(self) -> AccountInfo:
        data = api_get(self._client, "/wallet/cash")
        return AccountInfo(
            total_assets=0,  # kabuステーションは合計資産APIなし
            cash=data.get("StockAccountWallet", 0),
            market_value=0,
            unrealized_pnl=0,
            buying_power=data.get("StockAccountWallet", 0),
            raw=data,
        )

    def get_orders(self) -> list[dict[str, Any]]:
        data = api_get(self._client, "/orders")
        return data if isinstance(data, list) else []

    def get_deals(self) -> list[dict[str, Any]]:
        # kabuステーションには約定履歴専用APIがないため、注文から抽出
        orders = self.get_orders()
        return [o for o in orders if o.get("State") == 5]  # State=5: 全部約定

    def get_quote(self, code: str) -> dict[str, Any]:
        data = api_get(self._client, f"/board/{code}@1")  # @1 = 東証
        return {
            "code": code,
            "last_price": data.get("CurrentPrice", 0),
            "open": data.get("OpeningPrice", 0),
            "high": data.get("HighPrice", 0),
            "low": data.get("LowPrice", 0),
            "volume": data.get("TradingVolume", 0),
            "bid_price": data.get("BidPrice", 0),
            "ask_price": data.get("AskPrice", 0),
            "raw": data,
        }

    def get_kline(self, code: str, ktype: str = "K_1M", count: int = 100) -> list[dict[str, Any]]:
        # kabuステーションはKline APIのサポートが限定的
        raise NotImplementedError("kabuステーションのKline APIは未対応です")

    def close(self) -> None:
        self._client.close()
