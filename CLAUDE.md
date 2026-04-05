# CLAUDE.md

## プロジェクト概要

マルチデータソース対応の株式トレーディングツール。moomoo OpenAPI、J-Quants API、EDINET API、Google News RSS、X APIを統合し、相場データ取得・注文発注・ポートフォリオ管理・財務データ・開示情報・マーケットニュースをカバーする。

## 技術スタック

- Python 3.11
- moomoo-api (OpenD経由でmoomoo証券APIに接続)
- httpx (外部API通信、リトライ付き)
- FastAPI / uvicorn (REST API)
- pandas (データ処理)
- Redis (Pub/Sub通知)
- SQLite (データ蓄積, WALモード)
- Docker / Docker Compose

## アーキテクチャ

```
src/                            # 共通ライブラリ（services/ と strategies/ が共用）
├── client.py                   # OpenD接続管理 (MoomooClient, コンテキストマネージャ対応)
├── market_data.py              # 相場データ取得 (quote, kline, orderbook, subscribe)
├── order.py                    # 注文管理 (指値/成行/逆指値/キャンセル)
├── portfolio.py                # ポジション・口座・注文一覧・約定照会
└── broker/                     # ブローカー抽象化
    ├── base.py                 # BrokerClient Protocol + OrderResult/Position/AccountInfo dataclass
    ├── moomoo_broker.py        # 既存 src/ 関数群をラップ
    └── factory.py              # create_broker() ファクトリ

services/
├── data/                       # Data Service (:8000) - BrokerClient経由の照会系 REST API
│   ├── main.py                 # FastAPI エンドポイント (8つ)
│   ├── config.py               # API_SECRET
│   ├── Dockerfile
│   └── requirements.txt
├── collector/                  # Collector Service - Kline 定期収集 (バッチ)
│   ├── main.py                 # エントリポイント、プロバイダ生成
│   ├── collector.py            # provider委譲、Redis Pub/Sub通知
│   ├── client.py               # OpenQuoteContext のみ（moomoo固有）
│   ├── config.py               # 環境変数設定
│   ├── db.py                   # SQLite (WALモード, UPSERT)
│   ├── watchlist.yml           # 収集対象銘柄 (provider フィールド付き)
│   ├── providers/              # KlineProvider 実装
│   │   ├── base.py             # KlineProvider Protocol
│   │   ├── moomoo.py           # moomoo OpenD
│   │   └── jquants.py          # J-Quants V2 daily bars
│   ├── Dockerfile
│   └── requirements.txt
├── fundamentals/               # Fundamentals Service (:8001) - J-Quants財務データ
│   ├── main.py                 # FastAPI + バックグラウンド収集
│   ├── jquants_client.py       # J-Quants API (statements, info, announcement)
│   ├── collector.py            # 定期収集 + Redis通知
│   ├── db.py                   # statements, listed_info テーブル
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── disclosure/                 # Disclosure Service (:8002) - EDINET開示情報
│   ├── main.py                 # FastAPI + 日次収集
│   ├── edinet_client.py        # EDINET API v2
│   ├── collector.py            # 日次収集 + Redis通知
│   ├── db.py                   # documents, filings テーブル
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
└── news/                       # News Service (:8003) - マーケットニュース収集
    ├── main.py                 # FastAPI + 定期収集
    ├── collector.py            # プロバイダ巡回 + Redis通知
    ├── db.py                   # articles テーブル
    ├── config.py
    ├── providers/              # NewsProvider 実装
    │   ├── base.py             # NewsProvider Protocol
    │   ├── google_news.py      # Google News RSS
    │   └── x.py                # X (Twitter) API v2
    ├── Dockerfile
    └── requirements.txt

shared/
├── kline_reader.py             # SQLite からの Kline 読み取り (読み取り専用, query_only=ON)
├── utils.py                    # df_to_records 等
├── http_client.py              # httpx + リトライの共通クライアント
└── auth/
    └── token_manager.py        # J-Quants API Key 認証 (V2)

strategies/
└── example.py                  # 移動平均クロスオーバー戦略サンプル (src/ を import)

scripts/
└── test_connection.py          # OpenD 疎通テスト

tests/                          # テストディレクトリ（未実装）
```

### 依存関係

- `src/` は共通ライブラリ。services/data, strategies が import する
- `src/broker/` は Protocol ベースの抽象化（moomoo 実装）
- `shared/` は全サービスから参照される共通ユーティリティ
- 各モジュールは `client.py` の `MoomooClient` のみに依存するフラット構造
- モジュール間の相互依存禁止
- collector は `KlineProvider` Protocol 経由でデータソースを抽象化

### Docker Compose 構成

| サービス | ポート | 依存 | ボリューム | env_file |
|----------|--------|------|-----------|----------|
| data | :8000 | - | - | .env |
| collector | なし | redis | kline-data:/data | .env |
| fundamentals | :8001 | redis | fundamentals-data:/data | .env |
| disclosure | :8002 | redis | disclosure-data:/data | .env |
| news | :8003 | redis | news-data:/data | .env |
| redis | :6379 | - | - | - |

- OpenD接続: `host.docker.internal:11111`

### データ参照方式

| 方式 | データソース | 用途 | 提供元 |
|------|-------------|------|--------|
| **ライブ参照** | OpenD | リアルタイム株価・板情報・現在の注文状態 | data service (BrokerClient経由) |
| **蓄積参照** | SQLite (collector経由) | 過去Kline・テクニカル指標計算・バックテスト | shared/kline_reader.py |
| **財務参照** | SQLite (fundamentals経由) | 財務諸表・銘柄情報・決算発表予定 | fundamentals service |
| **開示参照** | SQLite (disclosure経由) | 有報・大量保有報告書 | disclosure service |
| **ニュース参照** | SQLite (news経由) | マーケットニュース・X投稿 | news service |

- collector が OpenD/J-Quants → SQLite にデータを蓄積し、Redis Pub/Sub で通知
- 蓄積データは `shared/kline_reader.py` の `KlineReader` で読み取り専用アクセス

### API エンドポイント

**Data Service (:8000)** - 認証: `Authorization: Bearer {API_SECRET}`

- `GET /health` - ヘルスチェック（認証不要）
- `GET /quote/{code}` - リアルタイム株価
- `GET /kline/{code}` - ローソク足（ktype, count パラメータ）
- `GET /orderbook/{code}` - 板情報
- `GET /positions` - 保有ポジション
- `GET /account` - 口座情報
- `GET /orders` - 注文一覧
- `GET /deals` - 約定履歴

**Fundamentals Service (:8001)** - 認証: `Authorization: Bearer {API_SECRET}`

- `GET /health` - ヘルスチェック（認証不要）
- `GET /statements/{code}` - 財務情報
- `GET /info/{code}` - 銘柄情報
- `GET /announcement` - 決算発表予定
- `GET /stats` - 収集状況統計
- `GET /screening` - 成長銘柄スクリーニング (min_sales_growth, min_profit_growth, sector, market, limit)
- `GET /screening/consecutive-growth` - 連続増収増益 (min_periods, metric=both|sales|profit, sector, market, limit)
- `GET /screening/margin-improvement` - 営業利益率改善 (min_periods, min_margin_change, sector, market, limit)
- `GET /screening/forecast-revision` - 業績予想上方修正 (min_revision_pct, target=profit|sales|both, sector, market, limit)
- `GET /screening/eps-growth` - EPS成長率 (min_eps_growth, sector, market, limit)
- `GET /screening/quality` - 財務健全性 (min_equity_ratio, require_positive_cfo, require_negative_cfi, require_positive_fcf, min_roe, sector, market, limit)
- `GET /screening/multi-factor` - マルチファクタースコア (weights=JSON, sector, market, limit)
- `GET /screening/sector-relative` - セクター内偏差値 (sector33_code, factors=JSON, limit)
- `POST /bulk-collect` - 手動バルク収集トリガー (date_from, date_to)

**Disclosure Service (:8002)** - 認証: `Authorization: Bearer {API_SECRET}`

- `GET /health` - ヘルスチェック（認証不要）
- `GET /documents` - 書類一覧 (date, sec_code パラメータ)
- `GET /documents/{doc_id}` - 書類詳細
- `GET /documents/{doc_id}/download` - 書類ダウンロード

**News Service (:8003)** - 認証: `Authorization: Bearer {API_SECRET}`

- `GET /health` - ヘルスチェック（認証不要）
- `GET /articles` - 記事一覧 (provider, query, since, limit パラメータ)
- `GET /articles/{article_id}` - 記事詳細

### KlineProvider 切替

`watchlist.yml` の `provider` フィールドで銘柄ごとに指定:
- `moomoo`: moomoo OpenD (K_1M, K_5M, K_15M, K_DAY)
- `jquants`: J-Quants API (K_DAY のみ)

## よく使うコマンド

```bash
# Docker環境で全サービス起動
docker compose up -d

# 個別サービスのビルド・起動
docker compose build
docker compose up data collector
docker compose up fundamentals disclosure

# ローカル実行（疎通テスト）
python scripts/test_connection.py

# Docker経由で疎通テスト
docker compose run --rm data python -c "import src.broker; print('ok')"
```

## 環境変数

`.env.example` をコピーして `.env` を作成し、値を設定する。全サービスで共通の1ファイル。

| 変数 | サービス | 説明 |
|------|----------|------|
| `API_SECRET` | data, fundamentals, disclosure, news | Bearer認証トークン（必須） |
| `OPEND_HOST` | data, collector | OpenDホスト (default: host.docker.internal) |
| `OPEND_PORT` | data, collector | OpenDポート (default: 11111) |
| `TRADE_ENV` | data | SIMULATE / REAL (default: SIMULATE) |
| `TRADE_PASSWORD` | data | REAL環境でのアンロック用 |
| `REDIS_HOST` | collector, fundamentals, disclosure, news | Redis ホスト (default: redis) |
| `REDIS_PORT` | collector, fundamentals, disclosure, news | Redis ポート (default: 6379) |
| `DB_PATH` | collector | SQLiteパス (default: /data/klines.db) |
| `LOOP_INTERVAL` | collector | ポーリング間隔秒 (default: 30) |
| `JQUANTS_API_KEY` | collector, fundamentals | J-Quants API キー |
| `WATCHLIST_CODES` | fundamentals | 収集対象銘柄コード (カンマ区切り) |
| `BULK_COLLECT_ENABLED` | fundamentals | 全銘柄バルク収集有効化 (default: true) |
| `BULK_FETCH_DELAY` | fundamentals | バルク収集リクエスト間隔秒 (default: 13.0, Freeプラン対応) |
| `BULK_LOOKBACK_DAYS` | fundamentals | バルク収集遡り日数 (default: 730) |
| `EDINET_API_KEY` | disclosure | EDINET API キー |
| `NEWS_QUERIES` | news | 検索クエリ (カンマ区切り, default: 株式 マーケット,日経平均,stock market) |
| `X_BEARER_TOKEN` | news | X API Bearer Token (未設定でXプロバイダ無効) |

## 開発ルール

### OpenD前提 (moomooブローカー)

- moomooブローカー使用時は全API通信がホストのOpenD (port 11111) 経由
- Docker内からは `host.docker.internal:11111` で接続
- OpenDが起動していないとテスト不可

### moomoo API パターン

- API呼び出しは `(ret, data)` タプルを返す。必ず `ret != RET_OK` でエラーチェック
- リアルタイム株価取得 (`get_quote`) は事前に `subscribe()` が必要
- リアルタイムプッシュはハンドラクラス (`StockQuoteHandlerBase` 等) で受信

### J-Quants API パターン

- V2 API: API Key をヘッダー (`x-api-key`) に付与するだけで認証完了
- `shared/auth/token_manager.py` の `JQuantsAuth` クラスで認証ヘッダーを生成
- `shared/http_client.py` でリトライ付きリクエスト

### 市場制約

- US市場: 権限がないため現在使用不可 ("No Authority")
- HK市場: moomoo検証に使用 (HK.00700 等)
- JP市場: J-Quants, EDINETで使用
- `TRADE_ENV=SIMULATE` がデフォルト。本番切替は慎重に

### セキュリティ

- `.env` は絶対にコミットしない (`.gitignore` で除外済み)
- `.env.example` のみコミット対象（実際の値を含めない）
- `TRADE_PASSWORD` は環境変数経由のみ
- パブリックリポジトリのため、認証情報・個人情報をコードに含めない
- 各サービスはシークレット未設定時に起動を拒否する (fail-closed)
