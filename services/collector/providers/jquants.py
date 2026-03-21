"""J-Quants API V2 経由の Kline プロバイダ (日足のみ)"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

from shared.auth.token_manager import JQuantsAuth
from shared.http_client import create_http_client

logger = logging.getLogger(__name__)

JQUANTS_API_BASE = "https://api.jquants.com/v2"

# J-Quantsがサポートするタイムフレーム
SUPPORTED_TIMEFRAMES = {"K_DAY"}


class JQuantsProvider:
    """J-Quants V2 daily bars からKlineを取得する"""

    def __init__(self, auth: JQuantsAuth) -> None:
        self._auth = auth
        self._client = create_http_client(base_url=JQUANTS_API_BASE)
        self._subscription_end: datetime | None = None

    @property
    def name(self) -> str:
        return "jquants"

    def _parse_subscription_end(self, message: str) -> datetime | None:
        """400レスポンスからサブスクリプション終了日をパース"""
        match = re.search(r"~\s*(\d{4}-\d{2}-\d{2})", message)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d")
        return None

    def fetch_kline(self, code: str, timeframe: str, max_count: int) -> list[dict[str, Any]]:
        """J-Quants V2 daily bars からKlineを取得し、正規化して返す。

        code: J-Quants形式の銘柄コード (例: "72030" = トヨタ5桁コード)
        timeframe: "K_DAY" のみサポート
        """
        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise ValueError(f"J-QuantsはK_DAYのみ対応: {timeframe}")

        # max_count日分の期間を計算 (余裕を持って1.5倍の営業日)
        to_date = datetime.now()
        from_date = to_date - timedelta(days=int(max_count * 1.5) + 10)

        # サブスクリプション期間外のリクエストを防止
        if self._subscription_end and to_date > self._subscription_end:
            to_date = self._subscription_end
            from_date = to_date - timedelta(days=int(max_count * 1.5) + 10)

        params: dict[str, Any] = {
            "code": code,
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
        }

        headers = self._auth.get_auth_headers()
        resp = self._client.get(
            "/equities/bars/daily",
            params=params,
            headers=headers,
        )

        # サブスクリプション期間外の場合、終了日を学習してリトライ
        if resp.status_code == 400:
            body = resp.json()
            end_date = self._parse_subscription_end(body.get("message", ""))
            if end_date and self._subscription_end is None:
                self._subscription_end = end_date
                logger.info("J-Quants サブスクリプション終了日検出: %s", end_date.strftime("%Y-%m-%d"))
                return self.fetch_kline(code, timeframe, max_count)

        resp.raise_for_status()
        body = resp.json()

        quotes = body.get("data", [])
        if not quotes:
            logger.warning("J-Quants: %s のデータなし", code)
            return []

        # 新しい順にソートして max_count で切る
        quotes.sort(key=lambda x: x["Date"], reverse=True)
        quotes = quotes[:max_count]

        return [
            {
                "timestamp": q["Date"],
                "open": q.get("AdjO", q.get("O", 0)),
                "high": q.get("AdjH", q.get("H", 0)),
                "low": q.get("AdjL", q.get("L", 0)),
                "close": q.get("AdjC", q.get("C", 0)),
                "volume": q.get("Vo", 0),
                "turnover": q.get("Va", 0),
            }
            for q in quotes
        ]

    def close(self) -> None:
        self._client.close()
        self._auth.close()
