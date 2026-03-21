# Collector Service

Kline データを定期的に収集し SQLite に蓄積するバッチサービス。KlineProvider Protocol により moomoo OpenD / J-Quants API をデータソースとして切替可能。

## 責務

- `watchlist.yml` に定義された銘柄・タイムフレームの Kline を定期収集
- SQLite (`/data/klines.db`) に UPSERT で蓄積
- Redis Pub/Sub (`kline:{code}:{timeframe}`) で更新通知
- タイムフレームごとにポーリング間隔を自動制御

**責務外**: リアルタイム配信、REST API 提供、注文処理

## アーキテクチャ

```
watchlist.yml
    ↓
Main Loop (LOOP_INTERVAL)
    ↓
Collector → KlineProvider (moomoo / jquants) → Kline データ取得
    ↓                                            ↓
Redis Pub/Sub (通知)              SQLite UPSERT (蓄積)
                                       ↓
                              shared/kline_reader.py (外部参照)
```

### KlineProvider 切替

`watchlist.yml` の `provider` フィールドで銘柄ごとにデータソースを指定:

| Provider | 対応タイムフレーム | 用途 |
|----------|------------------|------|
| `moomoo` | K_1M, K_5M, K_15M, K_DAY | HK 市場等のリアルタイムデータ |
| `jquants` | K_DAY のみ | JP 市場の日足データ |

### ポーリング間隔

タイムフレームごとに最小ポーリング間隔を設定し、過剰なリクエストを防止:

| タイムフレーム | 最小間隔 |
|---------------|---------|
| K_1M | 60 秒 |
| K_5M | 300 秒 |
| K_15M | 900 秒 |
| K_DAY | 900 秒 |

## watchlist.yml

```yaml
targets:
  - code: "HK.00700"
    provider: moomoo
    timeframes: ["K_1M", "K_5M", "K_DAY"]

  - code: "7203"
    provider: jquants
    timeframes: ["K_DAY"]

defaults:
  provider: moomoo
  max_count: 200
  fetch_delay: 1.0
```

- `targets[].provider`: 省略時は `defaults.provider` を使用
- `targets[].timeframes`: 収集するタイムフレームのリスト
- `defaults.max_count`: 1 回の取得で取る最大本数
- `defaults.fetch_delay`: リクエスト間の待機秒数

## 環境変数

| 変数 | 必須 | デフォルト | 説明 |
|------|------|-----------|------|
| `OPEND_HOST` | No | `host.docker.internal` | OpenD ホスト（moomoo provider 使用時） |
| `OPEND_PORT` | No | `11111` | OpenD ポート |
| `REDIS_HOST` | No | `redis` | Redis ホスト |
| `REDIS_PORT` | No | `6379` | Redis ポート |
| `DB_PATH` | No | `/data/klines.db` | SQLite パス |
| `WATCHLIST_PATH` | No | `watchlist.yml` | 収集対象定義ファイル |
| `LOOP_INTERVAL` | No | `30` | メインループ間隔（秒） |
| `JQUANTS_REFRESH_TOKEN` | jquants 時 | - | J-Quants リフレッシュトークン |

## 依存関係

- Redis（Pub/Sub 通知）
- SQLite（WAL モード、ボリューム `kline-data` にマウント）
- OpenD（moomoo provider 使用時）
- J-Quants API（jquants provider 使用時、`shared/auth/token_manager.py` でトークン自動更新）

## Docker

```bash
# Redis と一緒に起動（Redis が healthy になるまで待機）
docker compose up -d collector

# ログ確認
docker compose logs -f collector
```

ポート公開なし（バッチサービス）。データは `kline-data` ボリュームに永続化。
