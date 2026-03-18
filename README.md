# moomoo-trader

moomoo OpenAPI を使った米国株トレーディングツール。相場データ取得から注文発注までをカバーする。

## 前提条件

- moomoo証券の口座（米国株取引対応）
- [OpenD](https://openapi.moomoo.com/moomoo-api-doc/en/) がローカルで起動していること
- Python 3.11+

## セットアップ

### Docker（推奨）

```bash
cp .env.example .env  # 設定値を記入
docker compose build
docker compose run --rm app python scripts/test_connection.py
```

### ローカル

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # 設定値を記入
```

> **Security**: `.env`には認証情報が含まれます。絶対にコミットしないでください。

## 構成

```
moomoo-trader/
├── src/
│   ├── __init__.py
│   ├── client.py        # OpenD接続・認証管理
│   ├── market_data.py   # 相場データ取得（株価、板、ローソク足）
│   ├── order.py         # 注文発注・管理（成行、指値、逆指値等）
│   └── portfolio.py     # ポジション・口座情報照会
├── strategies/
│   └── example.py       # サンプル戦略
├── scripts/
│   └── test_connection.py  # OpenD疎通テスト
├── tests/
│   └── __init__.py
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## 使い方

### 相場データ取得

```python
from src.client import create_client
from src.market_data import get_quote, get_kline

ctx = create_client()
quote = get_quote(ctx, "US.AAPL")
kline = get_kline(ctx, "US.AAPL", ktype="K_1M", count=100)
```

### 注文発注

```python
from src.order import place_limit_order, place_market_order

# 指値注文
place_limit_order(ctx, code="US.AAPL", side="BUY", qty=1, price=150.0)

# 成行注文
place_market_order(ctx, code="US.AAPL", side="BUY", qty=1)
```

### ポジション照会

```python
from src.portfolio import get_positions, get_account_info

positions = get_positions(ctx)
account = get_account_info(ctx)
```

## API制限

- 注文: 30秒間に最大15リクエスト / アカウント
- 米国株の価格精度: 小数点以下4桁
- 24時間取引帯は指値注文のみ（Day / GTC）

## 注意事項

- ライブ取引前にアカウントのアンロックが必要
- 必ずペーパートレードで動作確認してからライブに切り替えること
- OpenD がローカルで起動していないとAPI通信できない
