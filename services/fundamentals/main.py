"""Fundamentals Service - J-Quants 財務・銘柄情報 API + 定期収集"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

from collector import FundamentalsCollector
from config import (
    API_SECRET,
    BULK_COLLECT_ENABLED,
    JQUANTS_API_KEY,
    LOOP_INTERVAL,
    WATCHLIST_CODES,
)
from db import (
    compute_multi_factor_scores,
    compute_sector_relative_scores,
    create_connection,
    get_collection_stats,
    get_listed_info,
    get_statements,
    screen_consecutive_growth,
    screen_eps_growth,
    screen_forecast_revision,
    screen_growth_stocks,
    screen_margin_improvement,
    screen_quality_stocks,
)
from jquants_client import JQuantsFundamentalsClient
from shared.auth.token_manager import JQuantsAuth

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_collector: FundamentalsCollector | None = None
_collector_thread: threading.Thread | None = None
_bulk_thread: threading.Thread | None = None
_running = True


def _bulk_collect(collector: FundamentalsCollector) -> None:
    """初回バルク収集（バックグラウンド）"""
    try:
        collector.run_bulk()
    except Exception:
        logger.exception("バルク収集エラー")


def _collector_loop(collector: FundamentalsCollector) -> None:
    """バックグラウンドでの定期収集"""
    while _running:
        try:
            collector.run_once()
        except Exception:
            logger.exception("収集ループエラー")
        time.sleep(LOOP_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _collector, _collector_thread, _bulk_thread, _running

    if not API_SECRET:
        raise RuntimeError("API_SECRET が未設定です")
    if not JQUANTS_API_KEY:
        raise RuntimeError("JQUANTS_API_KEY が未設定です")

    conn = create_connection()
    auth = JQuantsAuth(JQUANTS_API_KEY)
    jq_client = JQuantsFundamentalsClient(auth)

    _collector = FundamentalsCollector(
        client=jq_client,
        conn=conn,
        codes=WATCHLIST_CODES,
    )

    # 初回バルク収集（全銘柄マスタが少ない or 決算データが少ない場合）
    if BULK_COLLECT_ENABLED:
        stats = get_collection_stats(conn)
        if stats["total_listed"] < 100 or stats["total_companies"] < 100:
            logger.info(
                "バルク収集を開始します (マスタ: %d銘柄, 決算: %d社)",
                stats["total_listed"], stats["total_companies"],
            )
            _bulk_thread = threading.Thread(target=_bulk_collect, args=(_collector,), daemon=True)
            _bulk_thread.start()
        else:
            logger.info(
                "既存データ十分 (マスタ: %d銘柄, 決算: %d社/%d件) - バルク収集スキップ",
                stats["total_listed"], stats["total_companies"], stats["total_statements"],
            )

    _collector_thread = threading.Thread(target=_collector_loop, args=(_collector,), daemon=True)
    _collector_thread.start()
    logger.info("Fundamentals Service 起動完了 (watchlist: %d銘柄, bulk: %s)", len(WATCHLIST_CODES), BULK_COLLECT_ENABLED)

    yield

    _running = False
    logger.info("Fundamentals Service 停止中...")
    _collector.close()
    jq_client.close()
    conn.close()


app = FastAPI(title="Fundamentals Service", lifespan=lifespan)


def verify_token(request: Request) -> None:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {API_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")


def get_conn():
    if _collector is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    return _collector.conn


@app.get("/health")
def health():
    return {"status": "ok", "service": "fundamentals"}


@app.get("/statements/{code}")
def statements(
    code: str,
    limit: int = 20,
    _: None = Depends(verify_token),
):
    conn = get_conn()
    rows = get_statements(conn, code, limit=limit)
    return {"code": code, "count": len(rows), "data": rows}


@app.get("/info/{code}")
def info(code: str, _: None = Depends(verify_token)):
    conn = get_conn()
    row = get_listed_info(conn, code)
    if row is None:
        raise HTTPException(status_code=404, detail=f"銘柄 {code} が見つかりません")
    return {"code": code, "data": row}


@app.get("/announcement")
def announcement(_: None = Depends(verify_token)):
    if _collector is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    try:
        rows = _collector.client.get_announcement()
        return {"count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/stats")
def stats(_: None = Depends(verify_token)):
    """収集状況の統計"""
    conn = get_conn()
    data = get_collection_stats(conn)
    data["bulk_running"] = _collector.is_bulk_running if _collector else False
    return {"data": data}


@app.get("/screening")
def screening(
    min_sales_growth: float = 0.0,
    min_profit_growth: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """成長銘柄スクリーニング"""
    conn = get_conn()
    rows = screen_growth_stocks(
        conn,
        min_sales_growth=min_sales_growth,
        min_profit_growth=min_profit_growth,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/consecutive-growth")
def screening_consecutive_growth(
    min_periods: int = 3,
    metric: str = "both",
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """連続増収増益スクリーニング"""
    conn = get_conn()
    rows = screen_consecutive_growth(
        conn,
        min_periods=min_periods,
        metric=metric,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/margin-improvement")
def screening_margin_improvement(
    min_periods: int = 2,
    min_margin_change: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """営業利益率改善スクリーニング"""
    conn = get_conn()
    rows = screen_margin_improvement(
        conn,
        min_periods=min_periods,
        min_margin_change=min_margin_change,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/forecast-revision")
def screening_forecast_revision(
    min_revision_pct: float = 0.0,
    target: str = "profit",
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """業績予想上方修正スクリーニング"""
    conn = get_conn()
    rows = screen_forecast_revision(
        conn,
        min_revision_pct=min_revision_pct,
        target=target,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/eps-growth")
def screening_eps_growth(
    min_eps_growth: float = 0.0,
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """EPS成長率スクリーニング"""
    conn = get_conn()
    rows = screen_eps_growth(
        conn,
        min_eps_growth=min_eps_growth,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/quality")
def screening_quality(
    min_equity_ratio: float | None = None,
    require_positive_cfo: bool = False,
    require_negative_cfi: bool = False,
    require_positive_fcf: bool = False,
    min_roe: float | None = None,
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """財務健全性スクリーニング"""
    conn = get_conn()
    rows = screen_quality_stocks(
        conn,
        min_equity_ratio=min_equity_ratio,
        require_positive_cfo=require_positive_cfo,
        require_negative_cfi=require_negative_cfi,
        require_positive_fcf=require_positive_fcf,
        min_roe=min_roe,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {"count": len(rows), "data": rows}


@app.get("/screening/multi-factor")
def screening_multi_factor(
    weights: str = "",
    sector: str = "",
    market: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """マルチファクタースコアリング"""
    import json as _json

    parsed_weights: dict[str, float] | None = None
    if weights:
        try:
            parsed_weights = _json.loads(weights)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="weights は有効なJSON文字列である必要があります")

    conn = get_conn()
    rows = compute_multi_factor_scores(
        conn,
        weights=parsed_weights,
        sector=sector,
        market=market,
        limit=min(limit, 500),
    )
    return {
        "count": len(rows),
        "meta": {"weights": parsed_weights or {}},
        "data": rows,
    }


@app.get("/screening/sector-relative")
def screening_sector_relative(
    sector33_code: str = "",
    factors: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    """セクター内偏差値スクリーニング"""
    import json as _json

    parsed_factors: list[str] | None = None
    if factors:
        try:
            parsed_factors = _json.loads(factors)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="factors は有効なJSON文字列である必要があります")

    conn = get_conn()
    rows = compute_sector_relative_scores(
        conn,
        sector33_code=sector33_code,
        factors=parsed_factors,
        limit=min(limit, 500),
    )
    return {
        "count": len(rows),
        "meta": {"sector33_code": sector33_code, "factors": parsed_factors or []},
        "data": rows,
    }


@app.post("/bulk-collect")
def bulk_collect(_: None = Depends(verify_token)):
    """手動バルク収集トリガー（全銘柄マスタ取得 → 全銘柄決算データ収集）"""
    global _bulk_thread
    if _collector is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    if _collector.is_bulk_running:
        raise HTTPException(status_code=409, detail="バルク収集が既に実行中です")

    def _run():
        _collector.run_bulk()

    _bulk_thread = threading.Thread(target=_run, daemon=True)
    _bulk_thread.start()
    return {"status": "started"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
