import os
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
VNE_FILE = os.path.join(BASE_DIR, "data_clean", "vnexpress_clean.csv")
TN_FILE  = os.path.join(BASE_DIR, "data_clean", "thanhnienvn_clean.csv")
OUT_FILE = os.path.join(BASE_DIR, "data_clean", "merged_news.csv")

def main():
    # Đọc 2 file
    vne = pd.read_csv(VNE_FILE)
    tn  = pd.read_csv(TN_FILE)

    # Thêm cột nguồn
    vne["Source"] = "VNExpress"
    tn["Source"]  = "ThanhNien"

    # Gộp dữ liệu
    merged = pd.concat([vne, tn], ignore_index=True)

    # Khử trùng lặp theo Title + Date
    merged = merged.drop_duplicates(subset=["Title", "Date"], keep="first")

    # Sắp xếp theo ngày
    merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
    merged = merged.sort_values("Date").reset_index(drop=True)

    # Xuất ra file CSV
    merged.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    # Thông tin kết quả
    print("✅ MERGE DONE")
    print(f"📦 Tổng cộng: {len(merged)} bài viết")
    print(f"📁 File xuất: {OUT_FILE}")
    print("📰 Cột dữ liệu:", ", ".join(merged.columns))

if __name__ == "__main__":
    main()
