#!/usr/bin/env python3
import csv
import sys
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ===== 追加: 実行環境に応じたデータディレクトリ切替 =====
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))

CSV_PATH = DATA_DIR / "details_store.csv"

KEEP_COL_ORDER = [
    "id","site_key","fetched_at","title","start_date","deadline_date",
    "summary","processed_summary","amount","fund_name","source_url","page_url",
    "category1","category2","category3","deleted"
]

def read_rows(path):
    p = Path(path)
    if not p.exists():
        print(f"ERROR: {p} が見つかりません", file=sys.stderr)
        sys.exit(1)
    with p.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

def ensure_deleted_col(rows):
    changed = False
    for r in rows:
        if "deleted" not in r or r["deleted"] == "":
            r["deleted"] = "0"
            changed = True
    return changed

def norm_len(text):
    if not text:
        return 0
    # 連続空白を単一スペースに
    t = " ".join(text.split())
    return len(t)

def parse_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

def choose_keeper(indices, rows):
    """同一 source_url の候補から keeper の行インデックスを返す。"""
    # 1) summary の長さ最大 → 2) fetched_at が新しい → 3) 後勝ち
    scored = []
    for i in indices:
        r = rows[i]
        slen = norm_len(r.get("summary", ""))
        fdt  = parse_date(r.get("fetched_at",""))
        scored.append((slen, fdt or datetime.min, i))
    scored.sort(reverse=True)
    return scored[0][2]

def write_rows(path, rows):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    # 既存列を尊重しつつ、足りない列は追加
    fieldnames = list(rows[0].keys())
    for col in KEEP_COL_ORDER:
        if col not in fieldnames:
            fieldnames.append(col)

    # 理想順を優先
    ordered = [c for c in KEEP_COL_ORDER if c in fieldnames] + [c for c in fieldnames if c not in KEEP_COL_ORDER]

    tmp = p.with_suffix(p.suffix + ".tmp")
    bak = p.with_suffix(p.suffix + ".bak")

    # 一時ファイルに書く
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ordered)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in ordered})

    # 既存があれば .bak に退避（上書き許容）
    if p.exists():
        try:
            if bak.exists():
                bak.unlink()
        except Exception:
            pass
        p.replace(bak)

    # tmp → 本番へ
    tmp.replace(p)
    print(f"更新完了: {p}（バックアップ: {bak}）")

def main():
    rows = read_rows(CSV_PATH)
    if not rows:
        print("空ファイルのため何もしません。"); return

    ensure_deleted_col(rows)

    # すでに deleted=1 は対象外にしない（再評価で復活の可能性もあるので一旦全員候補）
    by_src = defaultdict(list)
    for idx, r in enumerate(rows):
        src = (r.get("source_url") or "").strip()
        if not src:
            continue
        by_src[src].append(idx)

    total_groups = 0
    total_marked = 0

    for src, idxs in by_src.items():
        if len(idxs) <= 1:
            continue
        total_groups += 1
        keeper = choose_keeper(idxs, rows)
        for i in idxs:
            rows[i]["deleted"] = "0" if i == keeper else "1"
        total_marked += (len(idxs) - 1)

    write_rows(CSV_PATH, rows)
    print(f"重複グループ: {total_groups} / 論理削除にした行: {total_marked}")

if __name__ == "__main__":
    main()
