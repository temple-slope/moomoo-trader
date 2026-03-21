"""Data Service - 相場データ・ポートフォリオ・注文照会 API"""

import logging
from contextlib import asynccontextmanager
from dataclasses import asdict

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

from config import API_SECRET, BROKER_TYPE
from src.broker import BrokerClient, create_broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_broker: BrokerClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _broker
    if not API_SECRET:
        raise RuntimeError("API_SECRET が未設定です。起動を中止します。")
    logger.info("Data Service 起動中... (broker=%s)", BROKER_TYPE)
    _broker = create_broker(BROKER_TYPE)
    logger.info("ブローカー接続完了: %s", _broker.name)
    yield
    logger.info("Data Service 停止中...")
    if _broker:
        _broker.close()
        _broker = None


app = FastAPI(title="Data Service", lifespan=lifespan)


def get_broker() -> BrokerClient:
    if _broker is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    return _broker


def verify_token(request: Request) -> None:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {API_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
def health():
    return {"status": "ok", "service": "data", "broker": _broker.name if _broker else "none"}


@app.get("/quote/{code:path}")
def quote(code: str, _: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        data = broker.get_quote(code)
        return {"code": code, "data": data}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/kline/{code:path}")
def kline(
    code: str,
    ktype: str = "K_1M",
    count: int = 100,
    _: None = Depends(verify_token),
    broker: BrokerClient = Depends(get_broker),
):
    try:
        data = broker.get_kline(code, ktype=ktype, count=count)
        return {"code": code, "ktype": ktype, "count": count, "data": data}
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/orderbook/{code:path}")
def orderbook(code: str, _: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        # orderbook はmoomoo固有。BrokerClientにない場合はget_quoteで代替
        if hasattr(broker, 'client'):
            from shared.utils import df_to_records
            from src.market_data import get_orderbook
            data = get_orderbook(broker.client, code)
            if isinstance(data, dict):
                return {"code": code, "data": data}
            return {"code": code, "data": df_to_records(data)}
        raise HTTPException(status_code=501, detail="このブローカーではorderbook未対応です")
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/positions")
def positions(_: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        data = broker.get_positions()
        return {"data": [asdict(p) for p in data]}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/account")
def account(_: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        data = broker.get_account_info()
        return {"data": asdict(data)}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/orders")
def orders(_: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        data = broker.get_orders()
        return {"data": data}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/deals")
def deals(_: None = Depends(verify_token), broker: BrokerClient = Depends(get_broker)):
    try:
        data = broker.get_deals()
        return {"data": data}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
