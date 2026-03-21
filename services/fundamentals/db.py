"""SQLiteスキーマ管理 - 財務・銘柄情報"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from config import DB_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS statements (
    code              TEXT NOT NULL,
    disclosed_date    TEXT NOT NULL,
    type_of_document  TEXT NOT NULL DEFAULT '',
    net_sales         REAL,
    operating_profit  REAL,
    ordinary_profit   REAL,
    profit            REAL,
    earnings_per_share REAL,
    total_assets      REAL,
    equity_to_asset_ratio REAL,
    book_value_per_share  REAL,
    cash_flows_from_operating REAL,
    cash_flows_from_investing REAL,
    cash_flows_from_financing REAL,
    result_dividend_per_share_annual REAL,
    forecast_net_sales REAL,
    forecast_operating_profit REAL,
    forecast_ordinary_profit  REAL,
    forecast_profit   REAL,
    forecast_earnings_per_share REAL,
    forecast_dividend_per_share_annual REAL,
    raw_json          TEXT NOT NULL DEFAULT '{}',
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code, disclosed_date, type_of_document)
);

CREATE INDEX IF NOT EXISTS idx_statements_code
    ON statements (code, disclosed_date DESC);

CREATE TABLE IF NOT EXISTS listed_info (
    code           TEXT NOT NULL,
    date           TEXT NOT NULL,
    company_name   TEXT NOT NULL DEFAULT '',
    company_name_english TEXT NOT NULL DEFAULT '',
    sector17_code  TEXT NOT NULL DEFAULT '',
    sector33_code  TEXT NOT NULL DEFAULT '',
    market_code    TEXT NOT NULL DEFAULT '',
    market_code_name TEXT NOT NULL DEFAULT '',
    scale_category TEXT NOT NULL DEFAULT '',
    raw_json       TEXT NOT NULL DEFAULT '{}',
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code, date)
);

CREATE INDEX IF NOT EXISTS idx_listed_info_code
    ON listed_info (code, date DESC);
"""


def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """WALモードでSQLite接続を作成"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


def upsert_statements(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    """財務情報をupsert"""
    if not rows:
        return 0

    import json

    sql = """
    INSERT OR REPLACE INTO statements
        (code, disclosed_date, type_of_document,
         net_sales, operating_profit, ordinary_profit, profit,
         earnings_per_share, total_assets, equity_to_asset_ratio,
         book_value_per_share,
         cash_flows_from_operating, cash_flows_from_investing, cash_flows_from_financing,
         result_dividend_per_share_annual,
         forecast_net_sales, forecast_operating_profit, forecast_ordinary_profit,
         forecast_profit, forecast_earnings_per_share,
         forecast_dividend_per_share_annual,
         raw_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            row.get("Code", ""),
            row.get("DiscDate", ""),
            row.get("DocType", ""),
            row.get("Sales"),
            row.get("OP"),
            row.get("OdP"),
            row.get("NP"),
            row.get("EPS"),
            row.get("TA"),
            row.get("EqAR"),
            row.get("BPS"),
            row.get("CFO"),
            row.get("CFI"),
            row.get("CFF"),
            row.get("DivAnn"),
            row.get("FSales"),
            row.get("FOP"),
            row.get("FOdP"),
            row.get("FNP"),
            row.get("FEPS"),
            row.get("FDivAnn"),
            json.dumps(row, ensure_ascii=False),
        )
        for row in rows
    ]
    conn.executemany(sql, params)
    conn.commit()
    return len(params)


def upsert_listed_info(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    """銘柄情報をupsert"""
    if not rows:
        return 0

    import json

    sql = """
    INSERT OR REPLACE INTO listed_info
        (code, date, company_name, company_name_english,
         sector17_code, sector33_code,
         market_code, market_code_name, scale_category,
         raw_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            row.get("Code", ""),
            row.get("Date", ""),
            row.get("CoName", ""),
            row.get("CoNameEn", ""),
            row.get("S17", ""),
            row.get("S33", ""),
            row.get("Mkt", ""),
            row.get("MktNm", ""),
            row.get("ScaleCat", ""),
            json.dumps(row, ensure_ascii=False),
        )
        for row in rows
    ]
    conn.executemany(sql, params)
    conn.commit()
    return len(params)


def get_statements(
    conn: sqlite3.Connection, code: str, limit: int = 20
) -> list[dict[str, Any]]:
    """財務情報を取得"""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        """
        SELECT * FROM statements
        WHERE code = ?
        ORDER BY disclosed_date DESC
        LIMIT ?
        """,
        (code, limit),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


def get_listed_info(
    conn: sqlite3.Connection, code: str
) -> dict[str, Any] | None:
    """最新の銘柄情報を取得"""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        """
        SELECT * FROM listed_info
        WHERE code = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (code,),
    )
    row = cursor.fetchone()
    conn.row_factory = None
    return dict(row) if row else None
