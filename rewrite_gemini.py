#!/usr/bin/env python3
import csv
from datetime import datetime
import os
import time
import google.generativeai as genai

# ========= 設定 =========
CSV_PATH = "details_store.csv"
BACKUP_PATH = "details_store_backup.csv"
ERROR_LOG = "rewrite_errors.log"
MODEL_NAME = "models/gemini-2.5-flash"  # list_models の結果に合わせて安定版を使用

# ========= APIキー =========
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    print("❌ 環境変数 GEMINI_API_KEY が設定されていません。"); exit(1)
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel(MODEL_NAME)

# ========= ユーティリティ =========
def backup_csv():
    with open(CSV_PATH, "r", encoding="utf-8") as src, open(BACKUP_PATH, "w", encoding="utf-8") as dst:
        dst.write(src.read())
    print(f"✅ バックアップ作成: {BACKUP_PATH}")

def generate_summary(summary_text: str) -> str:
    if not summary_text.strip():
        return ""
    prompt = f"""
あなたは助成金情報サイトの編集担当者です。
次の文章は、助成金や補助金の「対象・目的・概要」を説明する原文です。
この文章を、公式サイトに掲載できるように、自然で簡潔、かつ誤解のない日本語に書き直してください。

【目的】
・助成金情報サイトの概要欄にそのまま掲載できる形に整える
・一読して「どんな団体・活動が対象か」が伝わるようにする
・文末表現や語彙を統一し、読みやすい公的トーンに整える

【書き方のルール】
・箇条書き、装飾（**, --- など）、例示、説明、理由付けは禁止
・一段落（1〜3文）で完結する自然な文章にする
・文体は「〜です」「〜ます」調ではなく、公式文書の説明調（例：「〜を対象としています」など）
・固有名詞や条件がある場合は、意味を変えずに自然に整理して残す
・不明確な部分を補足する想像はしない（原文に忠実に）
・冗長な部分は削って要点を保つ

【出力形式】
・文章のみを出力。見出し、解説、注釈、余分なコメントは不要。

---
原文：
{summary_text}
---
"""
    try:
        res = model.generate_content(prompt)
        return (res.text or "").strip()
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as log:
            log.write(f"{datetime.now().isoformat()},ERROR,{str(e).replace(',', ' ')}\n")
        print(f"⚠️ AI生成中にエラー: {e}")
        return ""

# ========= メイン処理 =========
def main(force: bool):
    backup_csv()

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    out_rows = []
    updated = 0

    for idx, r in enumerate(rows, 1):
        summary = (r.get("summary") or "").strip()
        processed = (r.get("processed_summary") or "").strip()

        # 未処理のみ / --force なら全件
        if processed and not force:
            out_rows.append(r)
            continue
        if not summary:
            out_rows.append(r)
            continue

        text = generate_summary(summary)
        if text:
            r["processed_summary"] = text
            updated += 1
        else:
            # 失敗時は空欄のまま（コピー挿入などはしない）
            r["processed_summary"] = ""
        out_rows.append(r)

        # 軽いレート制御（速すぎ対策）
        time.sleep(0.3)

        # 進捗表示（タイトル or 先頭20文字）
        title = (r.get("title") or r.get("taitoru") or "")[:20]
        print(f"[{idx}] {title}... → {'OK' if text else 'SKIP'}")

    # 書き戻し（全行を出力）
    fieldnames = rows[0].keys() if rows else []
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(out_rows)

    print(f"\n✅ AIリライト完了: {updated} 件更新 / {CSV_PATH}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="すべての行を再リライトする")
    args = ap.parse_args()
    main(force=args.force)
