"""環境変数読み込み"""

import os

API_SECRET = os.getenv("API_SECRET", "")

JQUANTS_REFRESH_TOKEN = os.getenv("JQUANTS_REFRESH_TOKEN", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_PATH = os.getenv("DB_PATH", "/data/fundamentals.db")

# 収集対象銘柄コード (カンマ区切り)
_codes_str = os.getenv("WATCHLIST_CODES", "")
WATCHLIST_CODES: list[str] = [c.strip() for c in _codes_str.split(",") if c.strip()]

# 収集ループ間隔(秒) - デフォルト6時間
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "21600"))
