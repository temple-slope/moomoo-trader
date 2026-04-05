"""環境変数読み込み"""

import os

API_SECRET = os.getenv("API_SECRET", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_PATH = os.getenv("DB_PATH", "/data/news.db")

# Google News 検索クエリ（カンマ区切り）
NEWS_QUERIES = [q.strip() for q in os.getenv("NEWS_QUERIES", "株式 決算,株価 業績,日経平均,相場 見通し").split(",") if q.strip()]

# X (Twitter) API
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")

# 収集ループ間隔(秒) - デフォルト30分
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "1800"))

# X プロバイダ収集間隔(秒) - デフォルト6時間 (1日4回)
X_INTERVAL = int(os.getenv("X_INTERVAL", "21600"))

# 各プロバイダ呼び出し間隔(秒)
FETCH_DELAY = float(os.getenv("FETCH_DELAY", "1.0"))

# 株価影響フィルタ: タイトル/サマリーにこれらのキーワードを含む記事のみ保存
STOCK_KEYWORDS = [
    k.strip()
    for k in os.getenv(
        "STOCK_KEYWORDS",
        "株価,株式,決算,業績,上方修正,下方修正,増収,減収,増益,減益,"
        "日経平均,TOPIX,相場,配当,自社株買い,株式分割,TOB,M&A,合併,買収,"
        "金利,為替,円安,円高,利上げ,利下げ,IPO,上場,暴落,急騰,反発,下落,上昇,"
        "営業利益,純利益,売上高,経常利益,GDP,景気,インフレ",
    ).split(",")
    if k.strip()
]
