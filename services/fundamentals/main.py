"""Fundamentals Service - J-Quants 財務・銘柄情報 API + 定期収集"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

from collector import FundamentalsCollector
from config import API_SECRET, JQUANTS_API_KEY, LOOP_INTERVAL, WATCHLIST_CODES
from db import create_connection, get_listed_info, get_statements
from jquants_client import JQuantsFundamentalsClient
from shared.auth.token_manager import JQuantsAuth

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_collector: FundamentalsCollector | None = None
_collector_thread: threading.Thread | None = None
_running = True


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
    global _collector, _collector_thread, _running

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

    _collector_thread = threading.Thread(target=_collector_loop, args=(_collector,), daemon=True)
    _collector_thread.start()
    logger.info("Fundamentals Service 起動完了 (対象: %d銘柄)", len(WATCHLIST_CODES))

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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
