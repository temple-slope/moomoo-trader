# Disclosure Service

EDINET API から開示書類（有価証券報告書・大量保有報告書等）を収集・蓄積し、REST API で提供するサービス。

## 責務

- EDINET API v2 から開示書類メタデータを日次収集
- 書類ファイル（XBRL / PDF）のダウンロード・保存
- SQLite (`/data/disclosure.db`) に蓄積
- REST API で書類の検索・閲覧・ダウンロードを提供
- Redis Pub/Sub で収集完了を通知

**責務外**: 株価データ、財務分析、注文処理

## アーキテクチャ

```
                     ┌─────── Background Thread ───────┐
                     │                                  │
EDINET API v2 ← EdinetClient ← DisclosureCollector     │
                                    ↓           ↓      │
                              SQLite UPSERT   Redis     │
                              ファイル保存    Pub/Sub    │
                     └──────────────────────────────────┘
                                    ↓
Client → [Bearer認証] → FastAPI (:8002) → SQLite 参照 / ファイル配信
```

- バックグラウンドスレッド（デーモン）が `LOOP_INTERVAL` ごとに自動収集
- 過去 3 日分の書類メタデータを取得し、未ダウンロードの上位 50 件をダウンロード

## エンドポイント

| Method | Path | 認証 | 説明 | パラメータ |
|--------|------|------|------|-----------|
| GET | `/health` | 不要 | ヘルスチェック | |
| GET | `/documents` | 要 | 書類一覧 | `?date=2024-01-15&sec_code=7203&limit=50` |
| GET | `/documents/{doc_id}` | 要 | 書類詳細 | |
| GET | `/documents/{doc_id}/download` | 要 | 書類ダウンロード | zip 形式 |

## 使用例

```bash
# 日付で書類検索
curl -H "Authorization: Bearer $API_SECRET" "http://localhost:8002/documents?date=2024-01-15"

# 銘柄コードで絞り込み
curl -H "Authorization: Bearer $API_SECRET" "http://localhost:8002/documents?sec_code=7203"

# 書類詳細
curl -H "Authorization: Bearer $API_SECRET" http://localhost:8002/documents/S100ABC1

# 書類ダウンロード
curl -H "Authorization: Bearer $API_SECRET" -o doc.zip http://localhost:8002/documents/S100ABC1/download
```

## 環境変数

| 変数 | 必須 | デフォルト | 説明 |
|------|------|-----------|------|
| `API_SECRET` | Yes | - | Bearer 認証トークン。未設定時は起動拒否 |
| `EDINET_API_KEY` | Yes | - | EDINET API キー |
| `REDIS_HOST` | No | `redis` | Redis ホスト |
| `REDIS_PORT` | No | `6379` | Redis ポート |
| `DB_PATH` | No | `/data/disclosure.db` | SQLite パス |
| `DOWNLOAD_DIR` | No | `/data/downloads` | ダウンロードファイル保存先 |
| `FETCH_DELAY` | No | `2.0` | API リクエスト間隔（秒、レート制限対策） |
| `LOOP_INTERVAL` | No | `21600` | 収集間隔（秒、デフォルト 6 時間） |

## セットアップ

### EDINET API キーの取得

1. [EDINET API](https://disclosure2.edinet-fsa.go.jp/) でアカウント登録
2. API キーを発行
3. `.env` の `EDINET_API_KEY` に設定

### 収集対象の書類タイプ

以下の書類タイプを自動収集:

| コード | 書類種別 |
|--------|---------|
| 120 | 有価証券報告書 |
| 140 | 四半期報告書 |
| 160 | 半期報告書 |
| 350 | 大量保有報告書 |
| 360 | 大量保有報告書（変更） |

## Redis Pub/Sub チャネル

| チャネル | タイミング |
|---------|-----------|
| `disclosure:documents:update` | 書類メタデータの収集完了時 |

## データベーススキーマ

- `documents` テーブル: 書類メタデータ（doc_id, edinet_code, sec_code, filer_name, doc_type_code, filing_date, downloaded フラグ）
- `filings` テーブル: ダウンロード済みファイル情報（doc_id, file_type, file_path）

## 依存関係

- EDINET API v2
- `shared/http_client.py`（リトライ付き HTTP クライアント）
- Redis（Pub/Sub 通知）
- SQLite（WAL モード、ボリューム `disclosure-data` にマウント）

## Docker

```bash
# Redis と一緒に起動
docker compose up -d disclosure

# ログ確認
docker compose logs -f disclosure
```

ポート: `8002`（`DISCLOSURE_PORT` 環境変数で変更可能）。ダウンロードファイルは `disclosure-data` ボリューム内に永続化。
