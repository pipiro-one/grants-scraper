import re
import sys
import json
import yaml
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
import os

def _norm_date(text: str, default_year=None) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    # 例: 2025年9月1日 / 2025/9/1 / 9月1日 / 9/1
    m = re.search(r"(\d{4})[年/.\-]\s*(\d{1,2})[月/.\-]\s*(\d{1,2})", text)
    if m:
        y, mth, d = map(int, m.groups())
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except ValueError:
            return None
    m = re.search(r"(\d{1,2})[月/.\-]\s*(\d{1,2})", text)
    if m:
        mth, d = map(int, m.groups())
        y = default_year or datetime.now().year
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_detail(html: str, page_url: str, cfg: dict) -> dict:
    soup = BeautifulSoup(html, "lxml")
    out = {
        "title": None,
        "start_date": None,
        "deadline_date": None,
        "summary": None,
        "processed_summary": None,
        "amount": None,       # ← 助成金額を追加
        "source_url": None,
        "fund_name": None,
        "page_url": page_url,
        "fetched_at": datetime.now().strftime("%Y-%m-%d"),
        "category1": None,
        "category2": None,
        "category3": None,
        "deleted": None,
    }

    # タイトル
    h = soup.select_one(cfg["title_selector"])
    if h:
        out["title"] = h.get_text(strip=True)

    label_map = cfg.get("label_map", {})
    seps = cfg.get("range_separators", ["～", "〜", "-", "から", "~"])

    for row in soup.select(cfg["row_selector"]):
        lab = row.select_one(cfg["label_selector"])
        val = row.select_one(cfg["value_selector"])
        if not lab or not val:
            continue

        label = lab.get_text(strip=True)
        field = label_map.get(label)  # 完全一致
        if not field:
            continue

        text = val.get_text(" ", strip=True)

        if field == "source_url":
            a = val.find("a", href=True)
            if a:
                out["source_url"] = urljoin(page_url, a["href"])

        elif field == "summary":
            out["summary"] = text

        elif field == "fund_name":
            out["fund_name"] = text

        elif field == "amount":
            out["amount"] = text

        elif field == "period":
            raw = " ".join(text.split())
            part_start, part_end = None, None
            for sep in seps:
                if sep in raw:
                    left, right = raw.split(sep, 1)
                    part_start = _norm_date(left)
                    yhint = re.search(r"(\d{4})\s*年", left)
                    part_end = _norm_date(
                        right,
                        default_year=int(yhint.group(1)) if yhint else datetime.now().year,
                    )
                    break
            if part_start is None and part_end is None:
                part_start = _norm_date(raw)
            out["start_date"] = out["start_date"] or part_start
            out["deadline_date"] = out["deadline_date"] or part_end

        elif field == "start_date":
            out["start_date"] = _norm_date(text)

        elif field == "deadline_date":
            out["deadline_date"] = _norm_date(text)

    # 予備: タイトル末尾の「… 10/31」で締切補完（未取得時のみ）
    if not out["deadline_date"] and out["title"]:
        m = re.search(r"(\d{1,2})[./月]\s*(\d{1,2})日?", out["title"])
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            try:
                out["deadline_date"] = datetime(datetime.now().year, mm, dd).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return out

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 extract_details.py <html_file> <site_key>")
        sys.exit(1)

    # ====== 追加: DATA_DIR を基準に設定ファイル/相対HTMLパスを解決 ======
    SCRIPT_DIR = Path(__file__).resolve().parent
    DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))

    html_arg, site_key = sys.argv[1], sys.argv[2]
    html_path = Path(html_arg)
    if not html_path.is_absolute():
        # 引数が相対パスなら DATA_DIR を起点に（従来の絶対パスはそのまま）
        html_path = (DATA_DIR / html_path).resolve()

    try:
        cfg_path = (DATA_DIR / "detail_sites.yaml")
        cfgs = yaml.safe_load(cfg_path.open("r", encoding="utf-8")) or []
    except Exception as e:
        print(f"ERROR: detail_sites.yaml を読めません: {e}")
        sys.exit(1)

    cfg = next((c for c in cfgs if c.get("site_key") == site_key), None)
    if not cfg:
        print(f"ERROR: site_key='{site_key}' の設定が detail_sites.yaml に見つかりません")
        sys.exit(1)

    try:
        html = html_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"ERROR: HTMLファイルを読めません: {html_path} ({e})")
        sys.exit(1)

    data = extract_detail(html, "file://" + str(html_path), cfg)
    print(json.dumps(data, ensure_ascii=False, indent=2))
