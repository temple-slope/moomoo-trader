"""OpenD接続管理 (QuoteContextのみ)"""

from __future__ import annotations

import logging

from moomoo import OpenQuoteContext

from config import OPEND_HOST, OPEND_PORT

logger = logging.getLogger(__name__)


def create_quote_ctx() -> OpenQuoteContext:
    """OpenQuoteContextを生成して返す"""
    logger.info("OpenD接続: %s:%d", OPEND_HOST, OPEND_PORT)
    return OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
