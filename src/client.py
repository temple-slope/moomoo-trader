"""OpenD接続・認証管理"""

from __future__ import annotations

import os
from dataclasses import dataclass
from types import TracebackType

from moomoo import OpenQuoteContext, OpenSecTradeContext, SecurityFirm, TrdEnv

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class MoomooClient:
    quote_ctx: OpenQuoteContext
    trade_ctx: OpenSecTradeContext
    trd_env: TrdEnv

    def close(self) -> None:
        self.quote_ctx.close()
        self.trade_ctx.close()

    def __enter__(self) -> MoomooClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


def create_client(
    host: str | None = None,
    port: int | None = None,
    trade_env: str | None = None,
    trade_password: str | None = None,
    security_firm: str | None = None,
    filter_trdmarket: str | None = None,
) -> MoomooClient:
    host = host or os.getenv("OPEND_HOST", "127.0.0.1")
    port = port or int(os.getenv("OPEND_PORT", "11111"))
    env_str = (trade_env or os.getenv("TRADE_ENV", "REAL")).upper()

    if env_str == "SIMULATE":
        trd_env = TrdEnv.SIMULATE
    elif env_str == "REAL":
        trd_env = TrdEnv.REAL
    else:
        raise ValueError(f"TRADE_ENV の値が不正です: {env_str!r} ('SIMULATE' or 'REAL')")

    firm = security_firm or os.getenv("SECURITY_FIRM", "FUTUJP")
    market = filter_trdmarket or os.getenv("FILTER_TRD_MARKET", "JP")

    quote_ctx = OpenQuoteContext(host=host, port=port)
    trade_ctx = OpenSecTradeContext(
        host=host, port=port,
        security_firm=firm, filter_trdmarket=market,
    )

    if trd_env == TrdEnv.REAL:
        password = trade_password or os.getenv("TRADE_PASSWORD", "")
        if password:
            trade_ctx.unlock_trade(password)

    return MoomooClient(
        quote_ctx=quote_ctx,
        trade_ctx=trade_ctx,
        trd_env=trd_env,
    )
