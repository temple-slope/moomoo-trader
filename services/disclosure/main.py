"""Disclosure Service - EDINET開示情報 API + 日次収集"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from collector import DisclosureCollector
from config import API_SECRET, DOWNLOAD_DIR, EDINET_API_KEY, LOOP_INTERVAL
from db import create_connection, get_document_by_id, get_documents
from edinet_client import EdinetClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_collector: DisclosureCollector | None = None
_collector_thread: threading.Thread | None = None
_running = True


def _collector_loop(collector: DisclosureCollector) -> None:
    """バックグラウンドでの日次収集"""
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
    if not EDINET_API_KEY:
        raise RuntimeError("EDINET_API_KEY が未設定です")

    conn = create_connection()
    edinet_client = EdinetClient(EDINET_API_KEY)

    _collector = DisclosureCollector(client=edinet_client, conn=conn)

    _collector_thread = threading.Thread(target=_collector_loop, args=(_collector,), daemon=True)
    _collector_thread.start()
    logger.info("Disclosure Service 起動完了")

    yield

    _running = False
    logger.info("Disclosure Service 停止中...")
    _collector.close()
    edinet_client.close()
    conn.close()


app = FastAPI(title="Disclosure Service", lifespan=lifespan)


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
    return {"status": "ok", "service": "disclosure"}


@app.get("/documents")
def documents(
    date: str = "",
    sec_code: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    conn = get_conn()
    rows = get_documents(conn, date=date, sec_code=sec_code, limit=limit)
    return {"count": len(rows), "data": rows}


@app.get("/documents/{doc_id}")
def document_detail(doc_id: str, _: None = Depends(verify_token)):
    conn = get_conn()
    row = get_document_by_id(conn, doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"書類 {doc_id} が見つかりません")
    return {"data": row}


@app.get("/documents/{doc_id}/download")
def document_download(doc_id: str, _: None = Depends(verify_token)):
    file_path = Path(DOWNLOAD_DIR) / f"{doc_id}.zip"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"ファイル未ダウンロード: {doc_id}")
    return FileResponse(path=str(file_path), filename=f"{doc_id}.zip")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
