# Fundamentals Service

J-Quants API から財務データを収集・蓄積し、REST API で提供するサービス。

## 責務

- J-Quants API から財務諸表・銘柄情報・決算発表予定を定期収集
- SQLite (`/data/fundamentals.db`) に蓄積
- REST API で財務データを照会可能にする
- Redis Pub/Sub で収集完了を通知

**責務外**: 株価データ、開示情報、注文処理

## アーキテクチャ

```
                    ┌─────── Background Thread ───────┐
                    │                                  │
J-Quants API ← JQuantsClient ← FundamentalsCollector  │
                                    ↓           ↓     │
                              SQLite UPSERT   Redis   │
                                    ↓         Pub/Sub  │
                    └──────────────────────────────────┘
                                    ↓
Client → [Bearer認証] → FastAPI (:8001) → SQLite 参照
```

- バックグラウンドスレッド（デーモン）が `LOOP_INTERVAL` ごとに自動収集
- REST API はフォアグラウンドで即座にレスポンス

## エンドポイント

| Method | Path | 認証 | 説明 | パラメータ |
|--------|------|------|------|-----------|
| GET | `/health` | 不要 | ヘルスチェック | |
| GET | `/statements/{code}` | 要 | 財務諸表 | `?limit=20` |
| GET | `/info/{code}` | 要 | 銘柄情報 | |
| GET | `/announcement` | 要 | 決算発表予定 | |

## 使用例

```bash
# 財務諸表
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8001/statements/7203

# 銘柄情報
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8001/info/7203

# 決算発表予定
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8001/announcement
```

## 環境変数

| 変数 | 必須 | デフォルト | 説明 |
|------|------|-----------|------|
| `API_SECRET` | Yes | - | Bearer 認証トークン。未設定時は起動拒否 |
| `JQUANTS_REFRESH_TOKEN` | Yes | - | J-Quants リフレッシュトークン |
| `WATCHLIST_CODES` | Yes | - | 収集対象銘柄コード（カンマ区切り、例: `7203,9984,6758`） |
| `REDIS_HOST` | No | `redis` | Redis ホスト |
| `REDIS_PORT` | No | `6379` | Redis ポート |
| `DB_PATH` | No | `/data/fundamentals.db` | SQLite パス |
| `LOOP_INTERVAL` | No | `21600` | 収集間隔（秒、デフォルト 6 時間） |

## セットアップ

### J-Quants リフレッシュトークンの取得

1. [J-Quants](https://jpx-jquants.com/) でアカウント登録
2. メールアドレス・パスワードで認証し、リフレッシュトークンを取得
3. `.env` の `JQUANTS_REFRESH_TOKEN` に設定

リフレッシュトークンは 1 週間有効。`shared/auth/token_manager.py` が ID トークン（24 時間有効）を自動更新する。

### 収集対象の設定

`WATCHLIST_CODES` にカンマ区切りで銘柄コード（4 桁）を指定:

```
WATCHLIST_CODES=7203,9984,6758,8306
```

## Redis Pub/Sub チャネル

| チャネル | タイミング |
|---------|-----------|
| `fundamentals:{code}:statements` | 財務諸表の収集完了時 |
| `fundamentals:{code}:info` | 銘柄情報の収集完了時 |

## 依存関係

- J-Quants API（`shared/auth/token_manager.py` でトークン管理）
- `shared/http_client.py`（リトライ付き HTTP クライアント）
- Redis（Pub/Sub 通知）
- SQLite（WAL モード、ボリューム `fundamentals-data` にマウント）

## Docker

```bash
# Redis と一緒に起動
docker compose up -d fundamentals

# ログ確認
docker compose logs -f fundamentals
```

ポート: `8001`（`FUNDAMENTALS_PORT` 環境変数で変更可能）
