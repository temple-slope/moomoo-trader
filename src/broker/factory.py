"""ブローカーファクトリ"""

from __future__ import annotations

from typing import Any

from .base import BrokerClient


def create_broker(**kwargs: Any) -> BrokerClient:
    """moomoo ブローカーを生成"""
    from .moomoo_broker import MomooBroker
    return MomooBroker(**kwargs)
