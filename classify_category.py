#!/usr/bin/env python3
import csv
import os
import google.generativeai as genai
from datetime import datetime

INPUT_FILE = "details_store.csv"
BACKUP_FILE = "details_store_backup.csv"
MODEL_NAME = "models/gemini-2.5-flash"

# ====== Gemini 設定 ======
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise EnvironmentError("❌ 環境変数 GEMINI_API_KEY が設定されていません。")
genai.configure(api_key=api_key)

# ====== バックアップ ======
def backup_file(src, dst):
    with open(src, "r", encoding="utf-8") as fsrc, open(dst, "w", encoding="utf-8") as fdst:
        fdst.write(fsrc.read())
    print(f"✅ バックアップ作成: {dst}")

# ====== 分類関数 ======
def classify_categories(title, summary):
    prompt = f"""
あなたは助成金情報サイトの編集担当者です。
次の助成金の「タイトル」と「概要」を読み取り、以下のカテゴリ一覧から最も関連が深いものを1〜3個選んでください。
必ず1つは選んでください。

【カテゴリ一覧】
教育, 福祉, 環境, 医療, 地域活性, テクノロジー, 芸術文化, 国際協力,
子ども, スポーツ, ジェンダー, SDGs, 災害支援, 研究, その他

【出力ルール】
・出力は「カテゴリ1,カテゴリ2,カテゴリ3」のみ。
・カテゴリが1つの場合は1つだけ出力。
・「不明」や「わからない」は使用禁止。
・追加説明や解説は不要。

---
タイトル: {title}
概要: {summary}
---
"""
    try:
        response = genai.GenerativeModel(MODEL_NAME).generate_content(prompt)
        text = response.text.strip()
        cats = [c.strip() for c in text.split(",") if c.strip()]
        while len(cats) < 3:
            cats.append("")
        return cats[:3]
    except Exception as e:
        print(f"⚠️ 分類失敗: {e}")
        return ["", "", ""]

# ====== メイン処理 ======
def main():
    backup_file(INPUT_FILE, BACKUP_FILE)

    rows = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            title = r.get("title", "")
            summary = r.get("processed_summary") or r.get("summary", "")
            c1, c2, c3 = classify_categories(title, summary)
            r["category1"], r["category2"], r["category3"] = c1, c2, c3
            rows.append(r)

    if not rows:
        print("⚠️ CSVが空のため処理をスキップしました。")
        return

    with open(INPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ カテゴリ分類完了: {len(rows)} 件 → {INPUT_FILE}")

if __name__ == "__main__":
    main()
