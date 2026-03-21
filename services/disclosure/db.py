"""SQLiteスキーマ管理 - EDINET開示情報"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

from config import DB_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id             TEXT PRIMARY KEY,
    edinet_code        TEXT NOT NULL DEFAULT '',
    sec_code           TEXT NOT NULL DEFAULT '',
    filer_name         TEXT NOT NULL DEFAULT '',
    doc_type_code      TEXT NOT NULL DEFAULT '',
    doc_description    TEXT NOT NULL DEFAULT '',
    filing_date        TEXT NOT NULL DEFAULT '',
    period_start       TEXT NOT NULL DEFAULT '',
    period_end         TEXT NOT NULL DEFAULT '',
    submit_date_time   TEXT NOT NULL DEFAULT '',
    raw_json           TEXT NOT NULL DEFAULT '{}',
    downloaded         INTEGER NOT NULL DEFAULT 0,
    updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_documents_filing_date
    ON documents (filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_documents_sec_code
    ON documents (sec_code, filing_date DESC);

CREATE TABLE IF NOT EXISTS filings (
    doc_id       TEXT NOT NULL,
    file_type    TEXT NOT NULL,
    file_path    TEXT NOT NULL DEFAULT '',
    downloaded_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (doc_id, file_type),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
);
"""


def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """WALモードでSQLite接続を作成"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


def upsert_documents(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    """書類メタデータをupsert"""
    if not rows:
        return 0

    sql = """
    INSERT OR REPLACE INTO documents
        (doc_id, edinet_code, sec_code, filer_name,
         doc_type_code, doc_description, filing_date,
         period_start, period_end, submit_date_time, raw_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            row.get("docID", ""),
            row.get("edinetCode", ""),
            row.get("secCode", ""),
            row.get("filerName", ""),
            row.get("docTypeCode", ""),
            row.get("docDescription", ""),
            row.get("filingDate", ""),
            row.get("periodStart", ""),
            row.get("periodEnd", ""),
            row.get("submitDateTime", ""),
            json.dumps(row, ensure_ascii=False),
        )
        for row in rows
    ]
    conn.executemany(sql, params)
    conn.commit()
    return len(params)


def mark_downloaded(conn: sqlite3.Connection, doc_id: str, file_type: str, file_path: str) -> None:
    """ダウンロード済みとしてマーク"""
    conn.execute(
        "INSERT OR REPLACE INTO filings (doc_id, file_type, file_path) VALUES (?, ?, ?)",
        (doc_id, file_type, file_path),
    )
    conn.execute("UPDATE documents SET downloaded = 1 WHERE doc_id = ?", (doc_id,))
    conn.commit()


def get_documents(
    conn: sqlite3.Connection,
    date: str = "",
    sec_code: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """書類一覧を取得"""
    conditions = []
    params_list: list[Any] = []

    if date:
        conditions.append("filing_date = ?")
        params_list.append(date)
    if sec_code:
        conditions.append("sec_code = ?")
        params_list.append(sec_code)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params_list.append(limit)

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        f"SELECT * FROM documents {where} ORDER BY filing_date DESC LIMIT ?",
        params_list,
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


def get_document_by_id(conn: sqlite3.Connection, doc_id: str) -> dict[str, Any] | None:
    """書類1件を取得"""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,))
    row = cursor.fetchone()
    conn.row_factory = None
    return dict(row) if row else None
