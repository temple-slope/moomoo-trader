"""OpenD接続・認証管理"""

from __future__ import annotations

import os
from dataclasses import dataclass
from types import TracebackType

from dotenv import load_dotenv
from moomoo import OpenQuoteContext, OpenSecTradeContext, TrdEnv

load_dotenv()


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


def create_client() -> MoomooClient:
    host = os.getenv("OPEND_HOST", "127.0.0.1")
    port = int(os.getenv("OPEND_PORT", "11111"))
    env_str = os.getenv("TRADE_ENV", "SIMULATE")
    trd_env = TrdEnv.SIMULATE if env_str == "SIMULATE" else TrdEnv.REAL

    quote_ctx = OpenQuoteContext(host=host, port=port)
    trade_ctx = OpenSecTradeContext(host=host, port=port)

    if trd_env == TrdEnv.REAL:
        password = os.getenv("TRADE_PASSWORD", "")
        if password:
            trade_ctx.unlock_trade(password)

    return MoomooClient(
        quote_ctx=quote_ctx,
        trade_ctx=trade_ctx,
        trd_env=trd_env,
    )


def close_client(client: MoomooClient) -> None:
    client.close()
