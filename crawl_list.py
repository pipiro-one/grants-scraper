# crawl_list.py — 毎日再クロール & --force 対応版（ページ送り対応のまま）
import csv
import hashlib
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin, urlparse

import argparse
import time
import os
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import yaml

HEADERS = {"User-Agent": "GrantsScraper/0.1 (+https://example.org)"}

# ========= 追加: 実行環境に合わせて入出力ディレクトリを切替 =========
# 未設定なら従来通り（スクリプトのある場所/カレント）で動作。
# Cloud Run など書込制限のある環境では DATA_DIR=/tmp を指定してください。
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))

# 既存の定数はそのままの名称を維持（中身だけ Path 化）
LIST_INDEX = DATA_DIR / "list_index.csv"
LIST_SITES = DATA_DIR / "list_sites.yaml"
LIST_STORE = DATA_DIR / "list_store.csv"

MAX_PAGES = 50
# 既存値をデフォルトに、必要なら環境変数で上書き可能に
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "0.2"))

# ========= 追加: リトライ付きの requests セッション =========
def _requests_session():
    retry = Retry(
        total=3,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

SESSION = _requests_session()

def read_csv(path: str):
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: list, header: list[str]):
    p = Path(path)
    # 親ディレクトリが無い場合に備えて作成（コンテナでも安全）
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

def ensure_csv(path: str, header: list[str]):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        with p.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

def fetch(url: str) -> str:
    # タイムアウト20秒は従来通り。セッションにリトライ付与。
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.text

def load_site_map() -> dict:
    p = Path(LIST_SITES)
    if not p.exists():
        raise FileNotFoundError(f"{p} が見つかりません")
    with p.open("r", encoding="utf-8") as f:
        arr = yaml.safe_load(f) or []
    return {it["site_key"]: it for it in arr}

def collect_detail_urls(soup: BeautifulSoup, base_url: str, list_selector: str) -> list[str]:
    nodes = soup.select(list_selector)
    base_host = urlparse(base_url).netloc
    urls = []
    for n in nodes:
        a = n if n.name == "a" else n.select_one("a")
        if not a: continue
        href = a.get("href")
        if not href: continue
        abs_url = urljoin(base_url, href)
        if urlparse(abs_url).netloc != base_host:  # 外部ドメイン除外
            continue
        urls.append(abs_url)
    # 重複除去
    seen, out = set(), []
    for u in urls:
        if u in seen: continue
        seen.add(u); out.append(u)
    return out

def find_next_url(soup: BeautifulSoup, base_url: str, cfg: dict) -> str|None:
    # YAMLがあれば優先
    a = None
    if cfg.get("next_selector"):
        a = soup.select_one(cfg["next_selector"])
    # フォールバック: rel=next
    if not a:
        a = soup.select_one("a[rel='next']")
    # フォールバック: テキストに「次」
    if not a:
        for cand in soup.select("a"):
            txt = (cand.get_text() or "").strip()
            if any(t in txt for t in ("次へ", "次の", "次")):
                a = cand; break
    if not a: return None
    href = a.get("href")
    if not href: return None
    return urljoin(base_url, href)

def append_to_store(site_key: str, urls: list[str]) -> int:
    ensure_csv(LIST_STORE, ["id", "site_key", "url", "dedupe_key", "fetched_at", "processed"])
    existing = read_csv(LIST_STORE)
    existed_keys = {row["dedupe_key"] for row in existing}
    start_id = 1 + (max([int(r["id"]) for r in existing]) if existing else 0)

    to_append = []
    i = start_id
    today = datetime.now().strftime("%Y-%m-%d")
    for u in urls:
        key = hashlib.sha256(u.encode("utf-8")).hexdigest()
        if key in existed_keys:  # 既存はスキップ
            continue
        to_append.append({
            "id": i,
            "site_key": site_key,
            "url": u,
            "dedupe_key": key,
            "fetched_at": today,
            "processed": "0",
        })
        i += 1

    if to_append:
        p = Path(LIST_STORE)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["id","site_key","url","dedupe_key","fetched_at","processed"])
            for r in to_append:
                w.writerow(r)
    return len(to_append)

def should_process_row(row: dict, force: bool) -> bool:
    """processed=1でも updated_at が今日でなければ実行。--force なら常に実行。"""
    if force:
        return True
    today_str = date.today().strftime("%Y-%m-%d")
    # updated_at が今日ならスキップ、そうでなければ実行
    return row.get("updated_at") != today_str

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="processed=1 でも再クロールする")
    args = parser.parse_args()

    idx_rows = read_csv(LIST_INDEX)
    if not idx_rows:
        print(f"ERROR: {LIST_INDEX} が空です"); return
    try:
        site_map = load_site_map()
    except FileNotFoundError as e:
        print(f"ERROR: {e}"); return
    if not site_map:
        print("ERROR: list_sites.yaml が空です"); return

    added_total = 0
    today_str = date.today().strftime("%Y-%m-%d")

    for row in idx_rows:
        if not should_process_row(row, args.force):
            continue

        site_key = row["site_key"]
        base = row["url"]
        cfg = site_map.get(site_key)
        if not cfg:
            print(f"[WARN] YAML 未定義: site_key={site_key}"); continue
        list_selector = cfg.get("list_selector")
        if not list_selector:
            print(f"[WARN] list_selector 未設定: site_key={site_key}"); continue

        page_url = base
        pages = 0
        while page_url and pages < MAX_PAGES:
            try:
                html = fetch(page_url)
                soup = BeautifulSoup(html, "lxml")
                detail_urls = collect_detail_urls(soup, page_url, list_selector)
                added = append_to_store(site_key, detail_urls)
                added_total += added
                print(f"[OK] {site_key}: {page_url} で {len(detail_urls)}件抽出 / 新規{added}件")
                next_url = find_next_url(soup, page_url, cfg)
                if next_url == page_url:
                    next_url = None
                page_url = next_url
                pages += 1
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"[WARN] 取得失敗 {page_url}: {e}")
                break

        # 実行した印だけ更新（同日中の再実行はスキップされる）
        row["processed"] = "1"
        row["updated_at"] = today_str

    write_csv(
        LIST_INDEX, idx_rows,
        header=["site_key","site_name","updated_at","url","processed"]
    )
    print(f"合計 新規追加: {added_total} 件 → {LIST_STORE}")

if __name__ == "__main__":
    main()

