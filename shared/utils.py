"""共通ユーティリティ"""

from typing import Any


def df_to_records(df: Any) -> list[dict]:
    """DataFrame をレコードのリストに変換。None / 空の場合は空リストを返す。"""
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    return df.to_dict(orient="records")
