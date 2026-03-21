"""Collectorエントリポイント"""

from __future__ import annotations

import logging
import signal
import sys
import time

import yaml

from client import create_quote_ctx
from collector import Collector
from config import FMP_API_KEY, JQUANTS_API_KEY, LOOP_INTERVAL, WATCHLIST_PATH
from db import create_connection
from providers import FMPProvider, JQuantsProvider, MoomooProvider, YFinanceProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_running = True


def _signal_handler(sig: int, frame: object) -> None:
    global _running
    logger.info("シグナル %d 受信、停止します", sig)
    _running = False


def load_watchlist(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _build_providers(watchlist: dict) -> dict:
    """watchlist内で使用されるプロバイダのインスタンスを生成"""
    default_provider = watchlist.get("defaults", {}).get("provider", "moomoo")
    targets = watchlist.get("targets", [])

    needed = {default_provider}
    for target in targets:
        needed.add(target.get("provider", default_provider))

    providers = {}

    if "moomoo" in needed:
        quote_ctx = create_quote_ctx()
        providers["moomoo"] = MoomooProvider(quote_ctx)

    if "jquants" in needed:
        from shared.auth.token_manager import JQuantsAuth

        if not JQUANTS_API_KEY:
            logger.error("JQUANTS_API_KEY が未設定です")
            sys.exit(1)
        auth = JQuantsAuth(JQUANTS_API_KEY)
        providers["jquants"] = JQuantsProvider(auth)

    if "yfinance" in needed:
        providers["yfinance"] = YFinanceProvider()

    if "fmp" in needed:
        if not FMP_API_KEY:
            logger.error("FMP_API_KEY が未設定です")
            sys.exit(1)
        providers["fmp"] = FMPProvider(FMP_API_KEY)

    missing = needed - set(providers.keys())
    if missing:
        logger.error("未対応プロバイダ: %s", missing)
        sys.exit(1)

    return providers


def main() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    watchlist = load_watchlist(WATCHLIST_PATH)
    targets = watchlist.get("targets", [])
    defaults = watchlist.get("defaults", {})

    if not targets:
        logger.error("watchlistにターゲットがありません")
        sys.exit(1)

    logger.info("収集対象: %d 銘柄", len(targets))

    providers = _build_providers(watchlist)
    conn = create_connection()

    collector = Collector(
        providers=providers,
        conn=conn,
        targets=targets,
        defaults=defaults,
    )

    try:
        while _running:
            try:
                collector.run_once()
            except Exception:
                logger.exception("ループエラー（次回再試行）")
            time.sleep(LOOP_INTERVAL)
    finally:
        logger.info("クリーンアップ中...")
        collector.close()
        # MoomooProvider 経由で管理される quote_ctx は collector.close() で閉じない
        # 直接閉じる
        if "moomoo" in providers:
            providers["moomoo"]._quote_ctx.close()
        conn.close()
        logger.info("停止完了")


if __name__ == "__main__":
    main()
