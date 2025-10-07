import google.generativeai as genai
import os

# 環境変数からAPIキーを取得
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    print("❌ 環境変数 GEMINI_API_KEY が設定されていません。")
    exit(1)

genai.configure(api_key=api_key)

print("✅ 利用可能なモデル一覧:\n")

for model in genai.list_models():
    print(f"- {model.name}")
