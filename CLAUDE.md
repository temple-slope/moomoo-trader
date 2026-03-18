# CLAUDE.md

## プロジェクト概要

moomoo OpenAPI を使った株式トレーディングツール。相場データ取得・注文発注・ポートフォリオ管理をカバーする。

## 技術スタック

- Python 3.11
- moomoo-api (OpenD経由でmoomoo証券APIに接続)
- pandas (データ処理)
- Docker / Docker Compose

## アーキテクチャ

```
src/
├── client.py       # OpenD接続管理 (MoomooClient, コンテキストマネージャ対応)
├── market_data.py  # 相場データ取得 (quote, kline, orderbook, subscribe)
├── order.py        # 注文管理 (指値/成行/逆指値/キャンセル)
└── portfolio.py    # ポジション・口座・約定照会

strategies/         # トレーディング戦略
scripts/            # 運用スクリプト (疎通テスト等)
```

- 各モジュールは `client.py` の `MoomooClient` のみに依存するフラット構造
- モジュール間の相互依存禁止
- `strategies/` が `src/` の各モジュールを組み合わせて使う

## よく使うコマンド

```bash
# Docker環境で実行
docker compose build
docker compose run --rm app python scripts/test_connection.py

# ローカル実行
python scripts/test_connection.py
```

## 開発ルール

### OpenD前提

- 全API通信はホストのOpenD (port 11111) 経由
- Docker内からは `host.docker.internal:11111` で接続
- OpenDが起動していないとテスト不可

### moomoo API パターン

- API呼び出しは `(ret, data)` タプルを返す。必ず `ret != RET_OK` でエラーチェック
- リアルタイム株価取得 (`get_quote`) は事前に `subscribe()` が必要
- リアルタイムプッシュはハンドラクラス (`StockQuoteHandlerBase` 等) で受信

### 市場制約

- US市場: 権限がないため現在使用不可 ("No Authority")
- HK市場: 検証に使用 (HK.00700 等)
- `TRADE_ENV=SIMULATE` がデフォルト。本番切替は慎重に

### セキュリティ

- `.env` は絶対にコミットしない (`.gitignore` で除外済み)
- `TRADE_PASSWORD` は環境変数経由のみ
- パブリックリポジトリのため、認証情報・個人情報をコードに含めない
