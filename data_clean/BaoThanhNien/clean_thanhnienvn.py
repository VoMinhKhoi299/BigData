#!/usr/bin/env python3
# Python 3.12
# Làm sạch & chuẩn hoá dữ liệu ThanhNien.vn (giống VNExpress, chỉ giữ 4 cột)
import os, html
import pandas as pd
from bs4 import BeautifulSoup
from pandas.tseries.offsets import MonthBegin, MonthEnd

# ===== CẤU HÌNH =====
BASE_DIR = os.path.dirname(__file__)
RAW_FILE = os.path.join(BASE_DIR, "data_raw", "thanhnien_articles_raw.csv")
OUT_DIR  = os.path.join(BASE_DIR, "data_clean")
OUT_FILE = os.path.join(OUT_DIR, "thanhnienvn_clean.csv")

N_LAST_MONTHS = 3     # lấy 3 tháng gần nhất (bao gồm tháng hiện tại)
MIN_ROWS = 500         # đảm bảo đủ dữ liệu

# Gom nhóm chuyên mục gần giống nhau
CATEGORY_GROUP = {
    "Thời sự": ["Thời sự", "Chính trị", "Xã hội"],
    "Kinh tế": ["Kinh tế", "Tài chính", "Doanh nghiệp"],
    "Thế giới": ["Thế giới", "Quốc tế"],
    "Giải trí": ["Giải trí", "Văn hoá", "Phim ảnh"],
    "Thể thao": ["Thể thao", "Bóng đá"],
    "Giáo dục": ["Giáo dục", "Du học"],
    "Sức khỏe": ["Sức khỏe", "Y tế"],
    "Đời sống": ["Đời sống", "Gia đình"],
    "Công nghệ": ["Công nghệ", "Tin tức công nghệ"]
}

# ===== HÀM HỖ TRỢ =====

def clean_html(text: str) -> str:
    """Xoá thẻ HTML & decode ký tự đặc biệt"""
    if not isinstance(text, str): 
        return ""
    text = html.unescape(text)
    return BeautifulSoup(text, "html.parser").get_text(" ").strip()

def canon_category(cat: str) -> str:
    """Chuẩn hoá tên chuyên mục"""
    if not isinstance(cat, str): 
        return "Khác"
    for group, variants in CATEGORY_GROUP.items():
        for v in variants:
            if v.lower() in cat.lower():
                return group
    return cat.strip() or "Khác"

def pick_last_months(df, n=3):
    """Lấy đúng n tháng gần nhất (bao gồm tháng hiện tại)"""
    max_date = df["Date"].max()
    last_month = max_date.replace(day=1)             # giữ tháng hiện tại
    start = (last_month - MonthBegin(n-1)).replace(day=1)
    end = (last_month + MonthEnd(1)).replace(hour=23, minute=59)
    return start, end

# ===== MAIN =====

def main():
    if not os.path.exists(RAW_FILE):
        print("❌ Không tìm thấy file:", RAW_FILE)
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    df = pd.read_csv(RAW_FILE)

    if df.empty:
        print("❌ File rỗng.")
        return

    # Làm sạch text
    for c in ["Title", "Description", "Category"]:
        if c in df.columns:
            df[c] = df[c].apply(clean_html)

    # Chuẩn hoá ngày
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"]).copy()

    # Chuẩn hoá chuyên mục
    df["Category"] = df["Category"].apply(canon_category)

    # Khử trùng lặp
    df = df.drop_duplicates(subset=["Title", "Date"], keep="first")

    # Lọc 3 tháng gần nhất
    start, end = pick_last_months(df, N_LAST_MONTHS)
    df = df[(df["Date"] >= start) & (df["Date"] <= end)]

    # Giữ đúng 4 cột
    df = df[["Title", "Date", "Category", "Description"]]

    # Xuất file
    df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    # Thông tin kết quả
    print("✅ CLEAN DONE")
    print(f"📅 Khoảng thời gian lọc: {start.date()} → {end.date()}")
    print(f"📦 {len(df)} bài viết  →  {OUT_FILE}")

if __name__ == "__main__":
    main()
