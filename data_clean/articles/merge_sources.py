import os
import pandas as pd
from csv import QUOTE_ALL

BASE_DIR = os.path.dirname(__file__)
VNE_FILE = os.path.join(BASE_DIR, "data_clean", "vnexpress_clean.csv")
TN_FILE  = os.path.join(BASE_DIR, "data_clean", "thanhnienvn_clean.csv")
OUT_FILE = os.path.join(BASE_DIR, "data_clean", "merged_news.csv")

def clean_text(text):
    """Làm sạch mô tả và tiêu đề (xóa xuống dòng, tab, strip khoảng trắng)."""
    if pd.isna(text):
        return ""
    return str(text).replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()

def main():
    vne = pd.read_csv(VNE_FILE)
    tn  = pd.read_csv(TN_FILE)

    # Thêm nguồn
    vne["Source"] = "VNExpress"
    tn["Source"]  = "ThanhNien"

    merged = pd.concat([vne, tn], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Title", "Date"], keep="first")

    merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
    merged = merged.sort_values("Date").reset_index(drop=True)

    # 🔹 Chỉ làm sạch 2 cột Title + Description
    merged["Title"] = merged["Title"].apply(clean_text)
    merged["Description"] = merged["Description"].apply(clean_text)

    # Xuất CSV, không ghi dòng header
    merged.to_csv(OUT_FILE, index=False, header=False, encoding="utf-8-sig", quoting=QUOTE_ALL)

    print("✅ MERGE + CLEAN DONE (NO HEADER)")
    print(f"📦 Tổng cộng: {len(merged)} bài viết")
    print(f"📁 File xuất: {OUT_FILE}")

if __name__ == "__main__":
    main()
