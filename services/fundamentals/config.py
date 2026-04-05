"""環境変数 + watchlist.yml 読み込み"""

import os

import yaml

API_SECRET = os.getenv("API_SECRET", "")

JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_PATH = os.getenv("DB_PATH", "/data/fundamentals.db")

WATCHLIST_PATH = os.getenv("WATCHLIST_PATH", "shared/watchlist.yml")

# 収集ループ間隔(秒) - デフォルト6時間
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "21600"))

# バルク収集: リクエスト間隔(秒) - Freeプラン: 13秒 (5件/分)
BULK_FETCH_DELAY = float(os.getenv("BULK_FETCH_DELAY", "13.0"))

# バルク収集: 全銘柄収集を有効化
BULK_COLLECT_ENABLED = os.getenv("BULK_COLLECT_ENABLED", "true").lower() == "true"


def _load_watchlist_codes() -> list[str]:
    """watchlist.yml から jquants プロバイダの銘柄コードを抽出"""
    try:
        with open(WATCHLIST_PATH) as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        return []
    targets = data.get("targets", [])
    return [t["code"] for t in targets if t.get("provider") == "jquants"]


WATCHLIST_CODES: list[str] = _load_watchlist_codes()
