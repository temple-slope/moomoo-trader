"""News Service - マーケットニュース収集 API"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

from collector import NewsCollector
from config import API_SECRET, LOOP_INTERVAL, NEWS_QUERIES, X_BEARER_TOKEN
from db import create_connection, get_article_by_id, get_articles
from providers.google_news import GoogleNewsProvider
from providers.x import XProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_collector: NewsCollector | None = None
_collector_thread: threading.Thread | None = None
_running = True


def _collector_loop(collector: NewsCollector) -> None:
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

    conn = create_connection()

    # プロバイダ生成
    providers = {}
    providers["google_news"] = GoogleNewsProvider()

    x_provider = XProvider(bearer_token=X_BEARER_TOKEN)
    if x_provider._available:
        providers["x"] = x_provider

    _collector = NewsCollector(providers=providers, conn=conn, queries=NEWS_QUERIES)

    _collector_thread = threading.Thread(target=_collector_loop, args=(_collector,), daemon=True)
    _collector_thread.start()
    logger.info("News Service 起動完了 (providers: %s)", list(providers.keys()))

    yield

    _running = False
    logger.info("News Service 停止中...")
    _collector.close()
    conn.close()


app = FastAPI(title="News Service", lifespan=lifespan)


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
    return {"status": "ok", "service": "news"}


@app.get("/articles")
def articles(
    provider: str = "",
    query: str = "",
    since: str = "",
    limit: int = 100,
    _: None = Depends(verify_token),
):
    conn = get_conn()
    rows = get_articles(conn, provider=provider, query=query, since=since, limit=min(limit, 500))
    return {"count": len(rows), "data": rows}


@app.get("/articles/{article_id}")
def article_detail(article_id: str, _: None = Depends(verify_token)):
    conn = get_conn()
    row = get_article_by_id(conn, article_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"記事 {article_id} が見つかりません")
    return {"data": row}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)
