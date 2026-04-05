"""SQLiteスキーマ管理 - ニュース記事"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from config import DB_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    article_id     TEXT PRIMARY KEY,
    provider       TEXT NOT NULL,
    title          TEXT NOT NULL DEFAULT '',
    url            TEXT NOT NULL DEFAULT '',
    source         TEXT NOT NULL DEFAULT '',
    summary        TEXT NOT NULL DEFAULT '',
    query          TEXT NOT NULL DEFAULT '',
    published_at   TEXT NOT NULL DEFAULT '',
    collected_at   TEXT NOT NULL DEFAULT (datetime('now')),
    raw_json       TEXT NOT NULL DEFAULT '{}',
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_articles_provider
    ON articles (provider, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_articles_published
    ON articles (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_articles_query
    ON articles (query, published_at DESC);
"""


def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """WALモードでSQLite接続を作成"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


def upsert_articles(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    """記事をupsert"""
    if not rows:
        return 0

    sql = """
    INSERT OR REPLACE INTO articles
        (article_id, provider, title, url, source, summary, query, published_at, raw_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            row.get("article_id", ""),
            row.get("provider", ""),
            row.get("title", ""),
            row.get("url", ""),
            row.get("source", ""),
            row.get("summary", ""),
            row.get("query", ""),
            row.get("published_at", ""),
            row.get("raw_json", "{}"),
        )
        for row in rows
    ]
    conn.executemany(sql, params)
    conn.commit()
    return len(params)


def get_articles(
    conn: sqlite3.Connection,
    provider: str = "",
    query: str = "",
    since: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """記事一覧を取得"""
    conditions = []
    params_list: list[Any] = []

    if provider:
        conditions.append("provider = ?")
        params_list.append(provider)
    if query:
        conditions.append("query = ?")
        params_list.append(query)
    if since:
        conditions.append("published_at >= ?")
        params_list.append(since)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params_list.append(limit)

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        f"SELECT * FROM articles {where} ORDER BY published_at DESC LIMIT ?",
        params_list,
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


def get_article_by_id(conn: sqlite3.Connection, article_id: str) -> dict[str, Any] | None:
    """記事1件を取得"""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("SELECT * FROM articles WHERE article_id = ?", (article_id,))
    row = cursor.fetchone()
    conn.row_factory = None
    return dict(row) if row else None
