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


def get_collection_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """収集状況の統計を取得"""
    stats: dict[str, Any] = {}

    cursor = conn.execute("SELECT COUNT(DISTINCT code) FROM statements")
    stats["total_companies"] = cursor.fetchone()[0]

    cursor = conn.execute("SELECT COUNT(*) FROM statements")
    stats["total_statements"] = cursor.fetchone()[0]

    cursor = conn.execute("SELECT MIN(disclosed_date), MAX(disclosed_date) FROM statements")
    row = cursor.fetchone()
    stats["earliest_date"] = row[0] or ""
    stats["latest_date"] = row[1] or ""

    cursor = conn.execute("SELECT COUNT(DISTINCT code) FROM listed_info")
    stats["total_listed"] = cursor.fetchone()[0]

    return stats


def screen_growth_stocks(
    conn: sqlite3.Connection,
    min_sales_growth: float = 0.0,
    min_profit_growth: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """成長銘柄スクリーニング: 直近2期を比較して成長率を算出"""
    sql = """
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE profit IS NOT NULL AND net_sales IS NOT NULL
              AND type_of_document LIKE 'FY%'
    ),
    growth AS (
        SELECT
            cur.code,
            cur.disclosed_date,
            cur.net_sales AS cur_sales,
            prev.net_sales AS prev_sales,
            CASE WHEN prev.net_sales IS NOT NULL AND prev.net_sales >= 1000000
                 THEN (cur.net_sales - prev.net_sales) / prev.net_sales * 100
                 ELSE NULL END AS sales_growth,
            cur.profit AS cur_profit,
            prev.profit AS prev_profit,
            CASE WHEN prev.profit IS NOT NULL AND prev.profit >= 1000000
                 THEN (cur.profit - prev.profit) / prev.profit * 100
                 ELSE NULL END AS profit_growth,
            cur.operating_profit,
            cur.earnings_per_share AS eps,
            cur.equity_to_asset_ratio AS equity_ratio,
            cur.forecast_earnings_per_share AS forecast_eps,
            cur.forecast_net_sales,
            cur.forecast_profit
        FROM latest cur
        JOIN latest prev ON cur.code = prev.code AND cur.rn = 1 AND prev.rn = 2
    )
    SELECT g.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM growth g
    LEFT JOIN (
        SELECT code, company_name, market_code_name, sector33_code,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
        FROM listed_info
    ) li ON g.code = li.code AND li.rn = 1
    WHERE g.sales_growth >= ?
      AND g.profit_growth >= ?
    """
    params_list: list[Any] = [min_sales_growth, min_profit_growth]

    if sector:
        sql += " AND li.sector33_code = ?"
        params_list.append(sector)
    if market:
        sql += " AND li.market_code_name = ?"
        params_list.append(market)

    sql += " ORDER BY g.profit_growth DESC LIMIT ?"
    params_list.append(limit)

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql, params_list)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


# ---------------------------------------------------------------------------
# listed_info JOIN 共通サブクエリ
# ---------------------------------------------------------------------------
_LISTED_INFO_JOIN = """
    LEFT JOIN (
        SELECT code, company_name, market_code_name, sector33_code,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
        FROM listed_info
    ) li ON {alias}.code = li.code AND li.rn = 1
"""


def _append_sector_market_filter(
    sql: str, params: list[Any], sector: str, market: str
) -> str:
    if sector:
        sql += " AND li.sector33_code = ?"
        params.append(sector)
    if market:
        sql += " AND li.market_code_name = ?"
        params.append(market)
    return sql


def _execute_screening(
    conn: sqlite3.Connection, sql: str, params: list[Any]
) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


# ---------------------------------------------------------------------------
# B-1: 連続増収増益スクリーニング
# ---------------------------------------------------------------------------
def screen_consecutive_growth(
    conn: sqlite3.Connection,
    min_periods: int = 3,
    metric: str = "both",
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """連続増収増益スクリーニング: 直近N期で連続して売上/利益が増加している銘柄"""
    # min_periods は「比較する期数」なので、必要な決算データは min_periods + 1 期
    n_rows = min_periods + 1

    having_parts: list[str] = []
    if metric in ("sales", "both"):
        having_parts.append("SUM(sales_up) = COUNT(*)")
    if metric in ("profit", "both"):
        having_parts.append("SUM(profit_up) = COUNT(*)")
    if not having_parts:
        having_parts.append("SUM(profit_up) = COUNT(*)")
    having_clause = " AND ".join(having_parts)

    sql = f"""
    WITH ranked AS (
        SELECT code, disclosed_date, net_sales, profit,
               earnings_per_share, operating_profit,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE net_sales IS NOT NULL AND profit IS NOT NULL
              AND type_of_document LIKE 'FY%'
    ),
    diffs AS (
        SELECT cur.code, cur.rn,
               cur.disclosed_date,
               cur.net_sales, cur.profit,
               CASE WHEN cur.net_sales > prev.net_sales THEN 1 ELSE 0 END AS sales_up,
               CASE WHEN cur.profit > prev.profit THEN 1 ELSE 0 END AS profit_up
        FROM ranked cur
        JOIN ranked prev ON cur.code = prev.code AND cur.rn + 1 = prev.rn
        WHERE cur.rn <= ?
    ),
    streaks AS (
        SELECT code,
               SUM(sales_up) AS consecutive_sales_up,
               SUM(profit_up) AS consecutive_profit_up,
               COUNT(*) AS periods_compared
        FROM diffs
        GROUP BY code
        HAVING {having_clause}
    )
    SELECT s.code, s.consecutive_sales_up, s.consecutive_profit_up, s.periods_compared,
           latest.net_sales, latest.profit, latest.earnings_per_share AS eps,
           latest.operating_profit,
           li.company_name, li.market_code_name, li.sector33_code
    FROM streaks s
    JOIN (
        SELECT code, net_sales, profit, earnings_per_share, operating_profit,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE net_sales IS NOT NULL AND profit IS NOT NULL
              AND type_of_document LIKE 'FY%'
    ) latest ON s.code = latest.code AND latest.rn = 1
    {_LISTED_INFO_JOIN.format(alias='s')}
    WHERE 1=1
    """
    params_list: list[Any] = [min_periods]
    sql = _append_sector_market_filter(sql, params_list, sector, market)
    sql += " ORDER BY s.consecutive_profit_up DESC, latest.profit DESC LIMIT ?"
    params_list.append(limit)

    return _execute_screening(conn, sql, params_list)


# ---------------------------------------------------------------------------
# B-2: 営業利益率改善スクリーニング
# ---------------------------------------------------------------------------
def screen_margin_improvement(
    conn: sqlite3.Connection,
    min_periods: int = 2,
    min_margin_change: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """営業利益率改善スクリーニング: 直近と過去N期前の営業利益率差を比較"""
    sql = f"""
    WITH ranked AS (
        SELECT code, disclosed_date, net_sales, operating_profit,
               CASE WHEN net_sales > 0
                    THEN operating_profit * 1.0 / net_sales * 100
                    ELSE NULL END AS op_margin,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE net_sales IS NOT NULL AND operating_profit IS NOT NULL
              AND type_of_document LIKE 'FY%'
    ),
    margin_diff AS (
        SELECT cur.code,
               cur.disclosed_date AS latest_date,
               cur.op_margin AS latest_margin,
               prev.op_margin AS prev_margin,
               cur.op_margin - prev.op_margin AS margin_change,
               cur.net_sales, cur.operating_profit
        FROM ranked cur
        JOIN ranked prev ON cur.code = prev.code AND cur.rn = 1 AND prev.rn = ?
        WHERE cur.op_margin IS NOT NULL AND prev.op_margin IS NOT NULL
    )
    SELECT md.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM margin_diff md
    {_LISTED_INFO_JOIN.format(alias='md')}
    WHERE md.latest_margin > 0 AND md.margin_change >= ?
    """
    params_list: list[Any] = [min_periods, min_margin_change]
    sql = _append_sector_market_filter(sql, params_list, sector, market)
    sql += " ORDER BY md.margin_change DESC LIMIT ?"
    params_list.append(limit)

    return _execute_screening(conn, sql, params_list)


# ---------------------------------------------------------------------------
# B-3: 業績予想上方修正スクリーニング
# ---------------------------------------------------------------------------
def screen_forecast_revision(
    conn: sqlite3.Connection,
    min_revision_pct: float = 0.0,
    target: str = "profit",
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """業績予想上方修正スクリーニング: 最新決算の通期予想 vs 直近FY実績で乖離率を算出"""
    sql = f"""
    WITH latest_forecast AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE (forecast_profit IS NOT NULL AND forecast_profit != '')
           OR (forecast_net_sales IS NOT NULL AND forecast_net_sales != '')
    ),
    latest_fy AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE type_of_document LIKE 'FY%'
    ),
    revision AS (
        SELECT
            cur.code,
            cur.disclosed_date,
            cur.type_of_document,
            cur.forecast_net_sales,
            fy.net_sales AS prev_sales,
            CASE WHEN cur.forecast_net_sales IS NOT NULL AND cur.forecast_net_sales != ''
                      AND fy.net_sales IS NOT NULL AND fy.net_sales >= 1000000
                 THEN (cur.forecast_net_sales - fy.net_sales) / fy.net_sales * 100
                 ELSE NULL END AS sales_revision_pct,
            cur.forecast_profit,
            fy.profit AS prev_profit,
            CASE WHEN cur.forecast_profit IS NOT NULL AND cur.forecast_profit != ''
                      AND fy.profit IS NOT NULL AND fy.profit >= 1000000
                 THEN (cur.forecast_profit - fy.profit) / fy.profit * 100
                 ELSE NULL END AS profit_revision_pct,
            cur.forecast_operating_profit,
            cur.forecast_earnings_per_share AS forecast_eps
        FROM latest_forecast cur
        JOIN latest_fy fy ON cur.code = fy.code AND fy.rn = 1
        WHERE cur.rn = 1
    )
    SELECT r.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM revision r
    {_LISTED_INFO_JOIN.format(alias='r')}
    WHERE 1=1
    """
    params_list: list[Any] = []

    if target in ("profit", "both"):
        sql += " AND r.profit_revision_pct >= ?"
        params_list.append(min_revision_pct)
    if target in ("sales", "both"):
        sql += " AND r.sales_revision_pct >= ?"
        params_list.append(min_revision_pct)

    sql = _append_sector_market_filter(sql, params_list, sector, market)

    order_col = "r.profit_revision_pct" if target != "sales" else "r.sales_revision_pct"
    sql += f" ORDER BY {order_col} DESC LIMIT ?"
    params_list.append(limit)

    return _execute_screening(conn, sql, params_list)


# ---------------------------------------------------------------------------
# B-4: EPS成長率スクリーニング
# ---------------------------------------------------------------------------
def screen_eps_growth(
    conn: sqlite3.Connection,
    min_eps_growth: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """EPS成長率スクリーニング: 直近2期のEPS変化率でフィルタ"""
    sql = f"""
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE earnings_per_share IS NOT NULL
              AND type_of_document LIKE 'FY%'
    ),
    eps_growth AS (
        SELECT
            cur.code,
            cur.disclosed_date,
            cur.earnings_per_share AS cur_eps,
            prev.earnings_per_share AS prev_eps,
            CASE WHEN prev.earnings_per_share IS NOT NULL AND prev.earnings_per_share >= 1.0
                 THEN (cur.earnings_per_share - prev.earnings_per_share)
                      / prev.earnings_per_share * 100
                 ELSE NULL END AS eps_growth,
            cur.net_sales, cur.profit, cur.operating_profit,
            cur.forecast_earnings_per_share AS forecast_eps
        FROM latest cur
        JOIN latest prev ON cur.code = prev.code AND cur.rn = 1 AND prev.rn = 2
    )
    SELECT eg.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM eps_growth eg
    {_LISTED_INFO_JOIN.format(alias='eg')}
    WHERE eg.eps_growth >= ?
    """
    params_list: list[Any] = [min_eps_growth]
    sql = _append_sector_market_filter(sql, params_list, sector, market)
    sql += " ORDER BY eg.eps_growth DESC LIMIT ?"
    params_list.append(limit)

    return _execute_screening(conn, sql, params_list)


# ---------------------------------------------------------------------------
# C-1: 財務健全性（クオリティ）スクリーニング
# ---------------------------------------------------------------------------
def screen_quality_stocks(
    conn: sqlite3.Connection,
    min_equity_ratio: float | None = None,
    require_positive_cfo: bool = False,
    require_negative_cfi: bool = False,
    require_positive_fcf: bool = False,
    min_roe: float | None = None,
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """財務健全性スクリーニング: 自己資本比率・CF・ROEの複合フィルタ"""
    sql = f"""
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE type_of_document LIKE 'FY%'
    ),
    quality AS (
        SELECT
            code, disclosed_date,
            net_sales, operating_profit, profit,
            earnings_per_share AS eps,
            total_assets,
            equity_to_asset_ratio * 100 AS equity_ratio_pct,
            book_value_per_share AS bps,
            cash_flows_from_operating AS cfo,
            cash_flows_from_investing AS cfi,
            cash_flows_from_financing AS cff,
            cash_flows_from_operating + cash_flows_from_investing AS fcf,
            CASE WHEN total_assets IS NOT NULL
                      AND equity_to_asset_ratio IS NOT NULL
                      AND equity_to_asset_ratio > 0
                      AND total_assets > 0
                 THEN profit / (total_assets * equity_to_asset_ratio) * 100
                 ELSE NULL END AS roe_approx
        FROM latest
        WHERE rn = 1
    )
    SELECT q.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM quality q
    {_LISTED_INFO_JOIN.format(alias='q')}
    WHERE 1=1
    """
    params_list: list[Any] = []

    if min_equity_ratio is not None:
        sql += " AND q.equity_ratio_pct >= ?"
        params_list.append(min_equity_ratio)
    if require_positive_cfo:
        sql += " AND q.cfo > 0"
    if require_negative_cfi:
        sql += " AND q.cfi < 0"
    if require_positive_fcf:
        sql += " AND q.fcf > 0"
    if min_roe is not None:
        sql += " AND q.roe_approx >= ?"
        params_list.append(min_roe)

    sql = _append_sector_market_filter(sql, params_list, sector, market)
    sql += " ORDER BY q.roe_approx DESC NULLS LAST LIMIT ?"
    params_list.append(limit)

    return _execute_screening(conn, sql, params_list)


# ---------------------------------------------------------------------------
# D-1: マルチファクタースコアリング
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS: dict[str, float] = {
    "growth_sales": 0.15,
    "growth_profit": 0.20,
    "growth_eps": 0.15,
    "op_margin": 0.15,
    "equity_ratio": 0.10,
    "roe": 0.15,
    "fcf_positive": 0.10,
}


def _fetch_all_factors(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """全銘柄の直近2期データからファクターを算出"""
    sql = """
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY disclosed_date DESC) AS rn
        FROM statements
        WHERE type_of_document LIKE 'FY%'
    ),
    factors AS (
        SELECT
            cur.code,
            cur.disclosed_date,
            -- 成長率 (前期黒字かつ一定額以上のみ算出)
            CASE WHEN prev.net_sales IS NOT NULL AND prev.net_sales >= 1000000
                 THEN (cur.net_sales - prev.net_sales) / prev.net_sales * 100
                 ELSE NULL END AS growth_sales,
            CASE WHEN prev.profit IS NOT NULL AND prev.profit >= 1000000
                 THEN (cur.profit - prev.profit) / prev.profit * 100
                 ELSE NULL END AS growth_profit,
            CASE WHEN prev.earnings_per_share IS NOT NULL AND prev.earnings_per_share >= 1.0
                 THEN (cur.earnings_per_share - prev.earnings_per_share)
                      / prev.earnings_per_share * 100
                 ELSE NULL END AS growth_eps,
            -- 利益率
            CASE WHEN cur.net_sales IS NOT NULL AND cur.net_sales > 0
                 THEN cur.operating_profit * 1.0 / cur.net_sales * 100
                 ELSE NULL END AS op_margin,
            -- 財務健全性
            cur.equity_to_asset_ratio * 100 AS equity_ratio,
            CASE WHEN cur.total_assets IS NOT NULL
                      AND cur.equity_to_asset_ratio IS NOT NULL
                      AND cur.equity_to_asset_ratio > 0
                      AND cur.total_assets > 0
                 THEN cur.profit / (cur.total_assets * cur.equity_to_asset_ratio) * 100
                 ELSE NULL END AS roe,
            CASE WHEN cur.cash_flows_from_operating IS NOT NULL
                      AND cur.cash_flows_from_investing IS NOT NULL
                      AND (cur.cash_flows_from_operating + cur.cash_flows_from_investing) > 0
                 THEN 1.0 ELSE 0.0 END AS fcf_positive,
            -- 付加情報
            cur.net_sales, cur.profit, cur.operating_profit,
            cur.earnings_per_share AS eps
        FROM latest cur
        LEFT JOIN latest prev ON cur.code = prev.code AND prev.rn = 2
        WHERE cur.rn = 1
    )
    SELECT f.*,
           li.company_name, li.market_code_name, li.sector33_code
    FROM factors f
    LEFT JOIN (
        SELECT code, company_name, market_code_name, sector33_code,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
        FROM listed_info
    ) li ON f.code = li.code AND li.rn = 1
    """
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.row_factory = None
    return rows


def compute_multi_factor_scores(
    conn: sqlite3.Connection,
    weights: dict[str, float] | None = None,
    sector: str = "",
    market: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """マルチファクタースコアリング: 複数指標を正規化・加重合算してランキング"""
    import numpy as np
    import pandas as pd

    w = weights if weights else DEFAULT_WEIGHTS
    factor_names = list(w.keys())

    rows = _fetch_all_factors(conn)
    if not rows:
        return []

    df = pd.DataFrame(rows)

    # セクター・市場フィルタ
    if sector:
        df = df[df["sector33_code"] == sector]
    if market:
        df = df[df["market_code_name"] == market]
    if df.empty:
        return []

    # min-max 正規化 (1/99パーセンタイルでクリップ)
    for col in factor_names:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")
        series = df[col]
        low = series.quantile(0.01)
        high = series.quantile(0.99)
        clipped = series.clip(low, high)
        rng = high - low
        if rng > 0:
            df[f"{col}_norm"] = (clipped - low) / rng * 100
        else:
            df[f"{col}_norm"] = 50.0
        df[f"{col}_norm"] = df[f"{col}_norm"].fillna(50.0)

    # 加重合算
    df["total_score"] = sum(
        df[f"{col}_norm"] * weight for col, weight in w.items()
    )

    df = df.sort_values("total_score", ascending=False).head(limit)

    # 出力用カラム整理
    output_cols = [
        "code", "company_name", "market_code_name", "sector33_code",
        "total_score",
    ] + factor_names + [
        "net_sales", "profit", "operating_profit", "eps",
    ]
    existing_cols = [c for c in output_cols if c in df.columns]
    result = df[existing_cols].replace({np.nan: None})
    return result.to_dict(orient="records")


# ---------------------------------------------------------------------------
# D-2: セクター内相対評価（偏差値）
# ---------------------------------------------------------------------------
DEFAULT_FACTORS = ["growth_profit", "op_margin", "roe", "equity_ratio"]


def compute_sector_relative_scores(
    conn: sqlite3.Connection,
    sector33_code: str = "",
    factors: list[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """セクター内偏差値スクリーニング: 同業種内での偏差値を算出"""
    import numpy as np
    import pandas as pd

    factor_list = factors if factors else DEFAULT_FACTORS

    rows = _fetch_all_factors(conn)
    if not rows:
        return []

    df = pd.DataFrame(rows)

    if sector33_code:
        df = df[df["sector33_code"] == sector33_code]
    if df.empty:
        return []

    # セクター別偏差値
    result_frames: list[pd.DataFrame] = []
    for sector, group in df.groupby("sector33_code"):
        for col in factor_list:
            if col not in group.columns:
                group[f"{col}_dev"] = 50.0
                continue
            group[col] = pd.to_numeric(group[col], errors="coerce")
            series = group[col]
            mean = series.mean()
            std = series.std()
            if std > 0:
                group[f"{col}_dev"] = 50 + 10 * (series - mean) / std
            else:
                group[f"{col}_dev"] = 50.0
            group[f"{col}_dev"] = group[f"{col}_dev"].fillna(50.0)

        dev_cols = [f"{c}_dev" for c in factor_list if f"{c}_dev" in group.columns]
        group["composite_deviation"] = group[dev_cols].mean(axis=1)
        group["sector_rank"] = group["composite_deviation"].rank(
            ascending=False, method="min"
        ).astype(int)
        group["low_confidence"] = len(group) < 5
        result_frames.append(group)

    if not result_frames:
        return []

    result_df = pd.concat(result_frames)
    result_df = result_df.sort_values("composite_deviation", ascending=False).head(limit)

    output_cols = [
        "code", "company_name", "market_code_name", "sector33_code",
        "composite_deviation", "sector_rank", "low_confidence",
    ] + [f"{c}_dev" for c in factor_list] + factor_list + [
        "net_sales", "profit", "operating_profit", "eps",
    ]
    existing_cols = [c for c in output_cols if c in result_df.columns]
    result = result_df[existing_cols].replace({np.nan: None})
    return result.to_dict(orient="records")
