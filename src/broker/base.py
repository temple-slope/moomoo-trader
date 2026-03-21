"""BrokerClient Protocol + 共通データ型"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class OrderResult:
    """注文結果"""
    order_id: str
    code: str
    side: str  # "BUY" or "SELL"
    qty: float
    price: float
    status: str  # "submitted", "filled", "cancelled", "failed"
    message: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class Position:
    """保有ポジション"""
    code: str
    name: str
    qty: float
    cost_price: float
    market_value: float
    unrealized_pnl: float
    side: str = "LONG"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class AccountInfo:
    """口座情報"""
    total_assets: float
    cash: float
    market_value: float
    unrealized_pnl: float
    buying_power: float = 0
    raw: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class BrokerClient(Protocol):
    """ブローカー共通インターフェース"""

    @property
    def name(self) -> str:
        """ブローカー名 (例: "moomoo", "kabu")"""
        ...

    def place_limit_order(
        self, code: str, side: str, qty: float, price: float, remark: str = ""
    ) -> OrderResult:
        ...

    def place_market_order(
        self, code: str, side: str, qty: float, remark: str = ""
    ) -> OrderResult:
        ...

    def cancel_order(self, order_id: str) -> OrderResult:
        ...

    def get_positions(self) -> list[Position]:
        ...

    def get_account_info(self) -> AccountInfo:
        ...

    def get_orders(self) -> list[dict[str, Any]]:
        ...

    def get_deals(self) -> list[dict[str, Any]]:
        ...

    def get_quote(self, code: str) -> dict[str, Any]:
        ...

    def get_kline(self, code: str, ktype: str = "K_1M", count: int = 100) -> list[dict[str, Any]]:
        ...

    def close(self) -> None:
        ...
