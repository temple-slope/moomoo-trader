"""Kline収集ループ + Redis通知"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

import redis

from config import REDIS_HOST, REDIS_PORT
from providers.base import KlineProvider

logger = logging.getLogger(__name__)

# タイムフレームごとのポーリング間隔(秒)
POLL_INTERVALS: dict[str, int] = {
    "K_1M": 60,
    "K_5M": 300,
    "K_15M": 900,
    "K_DAY": 900,
}


class Collector:
    def __init__(
        self,
        providers: dict[str, KlineProvider],
        conn: sqlite3.Connection,
        targets: list[dict[str, Any]],
        defaults: dict[str, Any],
    ) -> None:
        self.providers = providers
        self.conn = conn
        self.targets = targets
        self.defaults = defaults
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        # {(code, timeframe): last_fetch_epoch}
        self._last_fetch: dict[tuple[str, str], float] = {}

    def _should_fetch(self, code: str, timeframe: str) -> bool:
        """ポーリング間隔に基づいてフェッチすべきか判定"""
        key = (code, timeframe)
        now = time.time()
        interval = POLL_INTERVALS.get(timeframe, 60)
        last = self._last_fetch.get(key, 0)
        return (now - last) >= interval

    def _mark_fetched(self, code: str, timeframe: str) -> None:
        self._last_fetch[(code, timeframe)] = time.time()

    def _get_provider(self, provider_name: str) -> KlineProvider:
        """プロバイダ名からインスタンスを取得"""
        provider = self.providers.get(provider_name)
        if provider is None:
            raise RuntimeError(f"未登録のプロバイダ: {provider_name}")
        return provider

    def _publish(self, code: str, timeframe: str, count: int) -> None:
        """Redis Pub/Subで更新通知"""
        channel = f"kline:{code}:{timeframe}"
        message = json.dumps({
            "code": code,
            "timeframe": timeframe,
            "count": count,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
        self.redis.publish(channel, message)
        logger.debug("publish %s: %s", channel, message)

    def run_once(self) -> None:
        """全ターゲットを1回巡回"""
        from db import upsert_klines

        fetch_delay = self.defaults.get("fetch_delay", 1.0)
        max_count = self.defaults.get("max_count", 200)
        default_provider = self.defaults.get("provider", "moomoo")

        for target in self.targets:
            code = target["code"]
            timeframes = target.get("timeframes", ["K_DAY"])
            target_max = target.get("max_count", max_count)
            provider_name = target.get("provider", default_provider)

            provider = self._get_provider(provider_name)

            for tf in timeframes:
                if not self._should_fetch(code, tf):
                    continue

                try:
                    rows = provider.fetch_kline(code, tf, target_max)
                    count = upsert_klines(self.conn, code, tf, rows)
                    self._mark_fetched(code, tf)

                    if count > 0:
                        self._publish(code, tf, count)

                    logger.info("collected %s %s (%s): %d rows", code, tf, provider_name, count)
                except Exception:
                    logger.exception("収集失敗 %s %s (%s)", code, tf, provider_name)

                time.sleep(fetch_delay)

    def close(self) -> None:
        for provider in self.providers.values():
            provider.close()
        self.redis.close()
