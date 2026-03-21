"""環境変数読み込み"""

import os

API_SECRET = os.getenv("API_SECRET", "")

BROKER_TYPE = os.getenv("BROKER_TYPE", "moomoo")
