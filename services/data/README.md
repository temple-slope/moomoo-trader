# Data Service

相場データ・ポートフォリオ照会用の REST API。moomoo OpenD 経由でリアルタイムデータを返す。

## 責務

- リアルタイム株価・板情報・ローソク足の取得
- ポジション・口座情報・注文一覧・約定履歴の照会
- Bearer トークン (`API_SECRET`) による認証ゲートウェイ

**責務外**: 注文発注、データ蓄積、バックグラウンド収集

## アーキテクチャ

```
Client → [Bearer認証] → FastAPI (:8000) → BrokerClient → OpenD
```

- `src/broker/factory.py` の `create_broker()` でブローカーを生成
- FastAPI lifespan でブローカー接続の初期化・クリーンアップを管理

## エンドポイント

| Method | Path | 認証 | 説明 | 備考 |
|--------|------|------|------|------|
| GET | `/health` | 不要 | ヘルスチェック | |
| GET | `/quote/{code}` | 要 | リアルタイム株価 | |
| GET | `/kline/{code}` | 要 | ローソク足 | `?ktype=K_1M&count=100` |
| GET | `/orderbook/{code}` | 要 | 板情報 | |
| GET | `/positions` | 要 | 保有ポジション | |
| GET | `/account` | 要 | 口座情報 | |
| GET | `/orders` | 要 | 注文一覧 | |
| GET | `/deals` | 要 | 約定履歴 | |

## 使用例

```bash
# ヘルスチェック
curl http://localhost:8000/health

# リアルタイム株価
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8000/quote/HK.00700

# ローソク足（5分足、直近50本）
curl -H "Authorization: Bearer $API_SECRET" "http://localhost:8000/kline/HK.00700?ktype=K_5M&count=50"

# 保有ポジション
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8000/positions
```

## 環境変数

| 変数 | 必須 | デフォルト | 説明 |
|------|------|-----------|------|
| `API_SECRET` | Yes | - | Bearer 認証トークン。未設定時は起動拒否 |
| `OPEND_HOST` | No | `host.docker.internal` | OpenD ホスト |
| `OPEND_PORT` | No | `11111` | OpenD ポート |
| `TRADE_ENV` | No | `SIMULATE` | 取引環境 (`SIMULATE` / `REAL`) |
| `TRADE_PASSWORD` | REAL 時 | - | 本番アンロック用パスワード |

## 依存関係

- `src/broker/` - BrokerClient Protocol + moomoo 実装
- `src/client.py` - MoomooClient
- `src/market_data.py`, `src/portfolio.py` - moomoo API ラッパー

## Docker

```bash
# 単独ビルド・起動
docker compose up -d data

# ログ確認
docker compose logs -f data
```

ポート: `8000`（`DATA_PORT` 環境変数で変更可能）
