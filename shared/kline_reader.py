"""SQLite Kline読み取りユーティリティ (戦略bot用)"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd


class KlineReader:
    """SQLiteからKlineデータを読み取る（読み取り専用）"""

    def __init__(self, db_path: str = "/data/klines.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path, timeout=10)
            self._conn.execute("PRAGMA query_only=ON")
        return self._conn

    def get_klines(
        self,
        code: str,
        timeframe: str,
        limit: int = 200,
    ) -> pd.DataFrame:
        """指定銘柄・タイムフレームのKlineをDataFrameで返す（新しい順）"""
        df = pd.read_sql_query(
            """
            SELECT timestamp, open, high, low, close, volume, turnover
            FROM klines
            WHERE code = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            self.conn,
            params=(code, timeframe, limit),
        )
        return df

    def get_latest(self, code: str, timeframe: str) -> dict[str, Any] | None:
        """最新の1本を辞書で返す。データなしならNone"""
        cursor = self.conn.execute(
            """
            SELECT timestamp, open, high, low, close, volume, turnover
            FROM klines
            WHERE code = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (code, timeframe),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))

    def list_codes(self) -> list[str]:
        """DB内の全銘柄コードを返す"""
        cursor = self.conn.execute("SELECT DISTINCT code FROM klines ORDER BY code")
        return [row[0] for row in cursor.fetchall()]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> KlineReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
