#!/usr/bin/env python3
import argparse, csv, sys, os
from pathlib import Path
from datetime import datetime
import yaml, requests
from requests.adapters import HTTPAdapter, Retry
from extract_details import extract_detail

# ====== 追加: 実行環境に合わせたデータディレクトリ切り替え ======
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))

LIST_STORE = DATA_DIR / "list_store.csv"
DETAILS_STORE = DATA_DIR / "details_store.csv"
DETAIL_SITES = DATA_DIR / "detail_sites.yaml"

FIELDS = [
    "id","site_key","fetched_at","title","start_date","deadline_date",
    "summary","processed_summary","amount","fund_name","source_url","page_url",
    "category1","category2","category3","deleted"
]

# ====== 追加: リトライ付き requests セッション ======
def _session_with_retry():
    retry = Retry(
        total=3,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "GrantsScraper/0.1 (+https://example.org)"})
    return s

SESSION = _session_with_retry()

def load_existing_fetched_at() -> dict[str, str]:
    p = Path(DETAILS_STORE)
    if not p.exists():
        return {}
    fetched = {}
    with p.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            page = (row.get("page_url") or "").strip()
            fa = (row.get("fetched_at") or "").strip()
            if page and fa:
                fetched[page] = fa
    return fetched

def read_list_store() -> list[dict]:
    p = Path(LIST_STORE)
    if not p.exists():
        print(f"{LIST_STORE} がありません", file=sys.stderr)
        sys.exit(1)
    with p.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def read_yaml() -> list[dict]:
    p = Path(DETAIL_SITES)
    return yaml.safe_load(p.open("r", encoding="utf-8")) or []

def ensure_details_store_header():
    p = Path(DETAILS_STORE)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        with p.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()
        return
    # 既存ファイルのヘッダが古い場合（amountなし等）はマイグレーション
    with p.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    if header != FIELDS:
        # 既存を読み直して足りない列を補って書き直す
        rows = []
        with p.open("r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                for k in FIELDS:
                    row.setdefault(k, "")
                rows.append(row)
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader(); w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="処理する件数 (0=全部)")
    ap.add_argument("--force", action="store_true", help="既存でも再抽出する")
    ap.add_argument("--site-key", help="特定サイトだけ処理（任意）")
    args = ap.parse_args()

    ensure_details_store_header()
    cfgs = read_yaml()
    site_cfg_map = {c["site_key"]: c for c in cfgs}
    list_rows = read_list_store()
    existing_fetched = load_existing_fetched_at()

    # 既存分を読み込み（上書きでなく再出力型）
    out_rows = []
    next_id = 1
    p = Path(DETAILS_STORE)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            for row in reader:
                row["id"] = str(next_id)
                # 欠けがちな列を補完（安全のため／見た目は従来と同じ）
                for k in FIELDS:
                    row.setdefault(k, "")
                out_rows.append(row)
                next_id += 1

    processed = 0
    for row in list_rows:
        if args.limit and processed >= args.limit:
            break
        site_key = row.get("site_key")
        url = row.get("url")
        if args.site_key and site_key != args.site_key:
            continue
        if not site_key or not url:
            continue

        cfg = site_cfg_map.get(site_key)
        if not cfg:
            print(f"[WARN] site_key={site_key} の定義が detail_sites.yaml にありません")
            continue

        # 既に同じ page_url が出力済みならスキップ（--forceで再取得）
        if not args.force and any(r["page_url"] == url for r in out_rows):
            continue

        try:
            html = SESSION.get(url, timeout=20).text
            data = extract_detail(html, url, cfg)
        except Exception as e:
            print(f"[ERROR] {url}: {e}")
            continue

        page_url = data.get("page_url") or url
        fetched_date = existing_fetched.get(page_url) or data.get("fetched_at") or datetime.now().strftime("%Y-%m-%d")

        out = {
            "id": str(next_id),
            "site_key": data.get("site_key") or site_key,
            "fetched_at": fetched_date,
            "title": data.get("title") or "",
            "start_date": data.get("start_date") or "",
            "deadline_date": data.get("deadline_date") or "",
            "summary": data.get("summary") or "",
            "processed_summary": data.get("processed_summary") or "",
            "amount": data.get("amount") or "",
            "fund_name": data.get("fund_name") or "",
            "source_url": data.get("source_url") or "",
            "page_url": page_url,
            "category1": data.get("category1") or "",
            "category2": data.get("category2") or "",
            "category3": data.get("category3") or "",
            "deleted": data.get("deleted") or "",
        }
        out_rows.append(out)
        next_id += 1
        processed += 1

    # 出力（親ディレクトリが無い場合に備えて作成）
    Path(DETAILS_STORE).parent.mkdir(parents=True, exist_ok=True)
    with Path(DETAILS_STORE).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"合計 {processed} 件処理 → {DETAILS_STORE}")

if __name__ == "__main__":
    main()
