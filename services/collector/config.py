"""環境変数から設定を読み込む"""

import os

OPEND_HOST = os.getenv("OPEND_HOST", "host.docker.internal")
OPEND_PORT = int(os.getenv("OPEND_PORT", "11111"))

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_PATH = os.getenv("DB_PATH", "/data/klines.db")

WATCHLIST_PATH = os.getenv("WATCHLIST_PATH", "shared/watchlist.yml")

# メインループ間隔(秒)
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "30"))

# J-Quants API
JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY", "")

# FMP API
FMP_API_KEY = os.getenv("FMP_API_KEY", "")
