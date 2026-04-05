"""環境変数読み込み"""

import os

API_SECRET = os.getenv("API_SECRET", "")

EDINET_API_KEY = os.getenv("EDINET_API_KEY", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_PATH = os.getenv("DB_PATH", "/data/disclosure.db")

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/data/downloads")

# API呼び出し間隔(秒) - EDINET レート制限対策
FETCH_DELAY = float(os.getenv("FETCH_DELAY", "2.0"))

# 遡り日数 - 初回は365、以降はデフォルト3
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

# 収集ループ間隔(秒) - デフォルト6時間
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "21600"))
