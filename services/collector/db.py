"""SQLiteスキーマ管理 + UPSERT"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from config import DB_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS klines (
    code       TEXT    NOT NULL,
    timeframe  TEXT    NOT NULL,
    timestamp  TEXT    NOT NULL,
    open       REAL    NOT NULL,
    high       REAL    NOT NULL,
    low        REAL    NOT NULL,
    close      REAL    NOT NULL,
    volume     INTEGER NOT NULL,
    turnover   REAL    NOT NULL DEFAULT 0,
    PRIMARY KEY (code, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_klines_lookup
    ON klines (code, timeframe, timestamp DESC);
"""


def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """WALモードでSQLite接続を作成"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


def upsert_klines(
    conn: sqlite3.Connection,
    code: str,
    timeframe: str,
    rows: list[dict[str, Any]],
) -> int:
    """Klineデータをupsert。挿入/更新された行数を返す"""
    if not rows:
        return 0

    sql = """
    INSERT OR REPLACE INTO klines
        (code, timeframe, timestamp, open, high, low, close, volume, turnover)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            code,
            timeframe,
            row["timestamp"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row.get("turnover", 0),
        )
        for row in rows
    ]
    conn.executemany(sql, params)
    conn.commit()
    count = len(params)
    logger.debug("upsert %s %s: %d rows", code, timeframe, count)
    return count


def get_klines(
    conn: sqlite3.Connection,
    code: str,
    timeframe: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """指定銘柄・タイムフレームのKlineを新しい順に取得"""
    cursor = conn.execute(
        """
        SELECT timestamp, open, high, low, close, volume, turnover
        FROM klines
        WHERE code = ? AND timeframe = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (code, timeframe, limit),
    )
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
