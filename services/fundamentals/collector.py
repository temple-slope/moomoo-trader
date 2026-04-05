"""財務データ定期収集 + Redis通知"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

import httpx
import redis

from config import BULK_FETCH_DELAY, REDIS_HOST, REDIS_PORT
from db import upsert_listed_info, upsert_statements
from jquants_client import JQuantsFundamentalsClient

logger = logging.getLogger(__name__)


class FundamentalsCollector:
    """J-Quants 財務データの定期収集"""

    def __init__(
        self,
        client: JQuantsFundamentalsClient,
        conn: sqlite3.Connection,
        codes: list[str],
        fetch_delay: float = 1.0,
        bulk_fetch_delay: float = BULK_FETCH_DELAY,
    ) -> None:
        self.client = client
        self.conn = conn
        self.codes = codes
        self.fetch_delay = fetch_delay
        self.bulk_fetch_delay = bulk_fetch_delay
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self._bulk_running = False

    @property
    def is_bulk_running(self) -> bool:
        return self._bulk_running

    def _publish(self, channel: str, data: dict[str, Any]) -> None:
        message = json.dumps(data, ensure_ascii=False)
        self.redis.publish(channel, message)
        logger.debug("publish %s", channel)

    def _get_all_codes(self) -> list[str]:
        """DBに登録済みの全銘柄コードを取得"""
        cursor = self.conn.execute("SELECT DISTINCT code FROM listed_info ORDER BY code")
        return [row[0] for row in cursor.fetchall()]

    # --- watchlist 銘柄の個別収集 ---

    def collect_statements(self) -> None:
        """watchlist銘柄の財務情報を収集"""
        for code in self.codes:
            try:
                rows = self.client.get_statements(code)
                count = upsert_statements(self.conn, rows)
                if count > 0:
                    self._publish(
                        f"fundamentals:{code}:statements",
                        {"code": code, "count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                    )
                logger.info("statements %s: %d rows", code, count)
            except Exception:
                logger.exception("statements収集失敗 %s", code)
            time.sleep(self.fetch_delay)

    def collect_listed_info(self) -> None:
        """watchlist銘柄の銘柄情報を収集"""
        for code in self.codes:
            try:
                rows = self.client.get_listed_info(code=code)
                count = upsert_listed_info(self.conn, rows)
                if count > 0:
                    self._publish(
                        f"fundamentals:{code}:info",
                        {"code": code, "count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                    )
                logger.info("listed_info %s: %d rows", code, count)
            except Exception:
                logger.exception("listed_info収集失敗 %s", code)
            time.sleep(self.fetch_delay)

    # --- 全銘柄バルク収集 ---

    def collect_all_listed_info(self) -> int:
        """全上場銘柄のマスタを一括取得"""
        try:
            rows = self.client.get_all_listed_info()
            count = upsert_listed_info(self.conn, rows)
            logger.info("全銘柄マスタ取得完了: %d 銘柄", count)
            if count > 0:
                self._publish(
                    "fundamentals:bulk:listed_info",
                    {"count": count, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")},
                )
            return count
        except Exception:
            logger.exception("全銘柄マスタ取得失敗")
            return 0

    def _fetch_with_retry(self, func, *args, max_retries: int = 5) -> Any:
        """429レート制限時にバックオフ付きリトライ（5分ブロック対応）"""
        for attempt in range(max_retries):
            try:
                return func(*args)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    # 初回26秒→60秒→120秒→300秒(5分ブロック回復)
                    wait = min(self.bulk_fetch_delay * (2 ** (attempt + 1)), 300)
                    logger.warning("429 レート制限 - %d秒待機 (retry %d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise
        return None  # unreachable

    def collect_statements_bulk_by_code(self) -> int:
        """全銘柄の決算データを銘柄ごとに収集 (Freeプラン対応)

        /fins/summary?code=CODE で銘柄ごとに取得。
        Freeプラン: 5件/分 → 13秒/リクエスト × ~3800銘柄 = 約13.7時間
        """
        all_codes = self._get_all_codes()
        if not all_codes:
            logger.warning("銘柄マスタが空のためバルク収集スキップ")
            return 0

        # 既に取得済みの銘柄をスキップ
        existing = set()
        cursor = self.conn.execute("SELECT DISTINCT code FROM statements")
        existing = {row[0] for row in cursor.fetchall()}
        remaining = [c for c in all_codes if c not in existing]

        self._bulk_running = True
        total = 0
        total_codes = len(remaining)

        logger.info(
            "バルク収集開始: %d 銘柄 (既存 %d スキップ, 推定 %.1f 時間)",
            total_codes, len(existing), total_codes * self.bulk_fetch_delay / 3600,
        )

        try:
            for i, code in enumerate(remaining, 1):
                try:
                    rows = self._fetch_with_retry(self.client.get_statements, code)
                    if rows:
                        count = upsert_statements(self.conn, rows)
                        total += count
                        if count > 0:
                            self._publish(
                                "fundamentals:bulk:statements",
                                {
                                    "code": code,
                                    "count": count,
                                    "progress": f"{i}/{total_codes}",
                                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                },
                            )
                    if i % 100 == 0:
                        logger.info("バルク収集進捗: %d/%d 銘柄 (累計 %d 件)", i, total_codes, total)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 403:
                        logger.debug("アクセス制限 %s - スキップ", code)
                    else:
                        logger.warning("バルク収集失敗 %s: %s", code, e)
                except Exception:
                    logger.exception("バルク収集失敗 %s", code)

                time.sleep(self.bulk_fetch_delay)
        finally:
            self._bulk_running = False

        logger.info("バルク収集完了: %d 銘柄 → %d 件", total_codes, total)
        return total

    def run_bulk(self) -> None:
        """初回バッチ: 全銘柄マスタ取得 → 全銘柄の決算データ収集"""
        self.collect_all_listed_info()
        time.sleep(self.bulk_fetch_delay)
        self.collect_statements_bulk_by_code()

    # --- 実行 ---

    def run_once(self) -> None:
        """定期実行: watchlist個別収集（バルク収集中はスキップ）"""
        if self._bulk_running:
            logger.info("バルク収集中のためwatchlist収集スキップ")
            return
        self.collect_statements()
        self.collect_listed_info()

    def close(self) -> None:
        self.redis.close()
