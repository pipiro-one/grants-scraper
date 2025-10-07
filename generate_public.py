#!/usr/bin/env python3
import csv
import os
from pathlib import Path

# ===== 追加: 実行環境に応じて入出力ディレクトリを切替 =====
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))

SRC = DATA_DIR / "details_store.csv"
DST = DATA_DIR / "public_store.csv"

# サイト表示に必要なカラム順だけ定義（お好みで）
PUBLIC_COLS = [
    "title", "start_date", "deadline_date",
    "summary", "amount", "fund_name", "source_url",
    "category", "fetched_at",
]

def norm_summary(s: str, maxlen=600):
    if not s:
        return ""
    s = " ".join(s.split())  # 連続空白・改行を畳む
    return s if len(s) <= maxlen else s[:maxlen - 1] + "…"

def main():
    if not SRC.exists():
        raise FileNotFoundError(f"{SRC} が見つかりません")

    # 読み込み
    with SRC.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    out = []
    for r in rows:
        if (r.get("deleted") or "0") == "1":
            continue

        # 要約は processed_summary を優先、なければ summary
        summary = r.get("processed_summary") or r.get("summary") or ""

        # category1〜3 を結合して category へ（空は除外）
        cats = [r.get("category1", ""), r.get("category2", ""), r.get("category3", "")]
        category = " / ".join([c for c in cats if c])

        item = {
            "title":        r.get("title", ""),
            "start_date":   r.get("start_date", ""),
            "deadline_date":r.get("deadline_date", ""),
            "summary":      norm_summary(summary),
            "amount":       r.get("amount", ""),
            "fund_name":    r.get("fund_name", ""),
            "source_url":   r.get("source_url", ""),
            "category":     category,
            "fetched_at":   r.get("fetched_at", ""),
        }
        out.append(item)

    # 出力先のディレクトリを確保
    DST.parent.mkdir(parents=True, exist_ok=True)

    # 書き込み
    with DST.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PUBLIC_COLS)
        w.writeheader()
        w.writerows(out)

    print(f"公開用CSV作成: {DST}（{len(out)} 行）")

if __name__ == "__main__":
    main()
