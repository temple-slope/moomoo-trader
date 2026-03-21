"""ブローカーファクトリ"""

from __future__ import annotations

import os
from typing import Any

from .base import BrokerClient


def create_broker(broker_type: str | None = None, **kwargs: Any) -> BrokerClient:
    """環境変数またはパラメータに基づいてブローカーを生成

    broker_type: "moomoo" or "kabu"
    """
    broker_type = (broker_type or os.getenv("BROKER_TYPE", "moomoo")).lower()

    if broker_type == "moomoo":
        from .moomoo_broker import MomooBroker
        return MomooBroker(**kwargs)

    if broker_type == "kabu":
        from .kabu_broker import KabuBroker
        api_password = kwargs.pop("api_password", None) or os.getenv("KABU_API_PASSWORD", "")
        host = kwargs.pop("host", None) or os.getenv("KABU_HOST", "host.docker.internal")
        port = int(kwargs.pop("port", None) or os.getenv("KABU_PORT", "18080"))
        return KabuBroker(api_password=api_password, host=host, port=port)

    raise ValueError(f"未対応のブローカー: {broker_type!r} ('moomoo' or 'kabu')")
