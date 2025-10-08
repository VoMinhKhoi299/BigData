import os
import pandas as pd
from pandas.tseries.offsets import MonthBegin, MonthEnd

# ===== CONFIG =====
BASE_DIR      = os.path.dirname(__file__)
RAW_CSV       = os.path.join(BASE_DIR, "data_raw", "vnexpress_articles_raw.csv")
OUT_DIR       = os.path.join(BASE_DIR, "data_clean")
OUT_CLEAN     = os.path.join(OUT_DIR, "clean_data.csv")
OUT_SUMMARY   = os.path.join(OUT_DIR, "summary.csv")

USE_ABSOLUTE_RANGE = False    # True = lọc theo khoảng cụ thể, False = lấy N tháng gần nhất
ABS_FROM = "2025-01"          # nếu dùng khoảng tuyệt đối
ABS_TO   = "2025-06"

N_LAST_FULL_MONTHS = 3        # số tháng nguyên gần nhất cần lấy
MIN_ROWS           = 1000     # số dòng tối thiểu để đảm bảo dữ liệu đủ lớn

CATEGORY_CANON = {
    "Thời sự": ["Thời sự", "Thoi su"],
    "Kinh doanh": ["Kinh doanh", "Kinh tế"],
    "Thế giới": ["Thế giới", "The gioi"],
    "Giải trí": ["Giải trí", "Giai tri"],
    "Thể thao": ["Thể thao", "The thao"],
}

# ====== SUPPORT ======

def month_start_str(ym: str) -> pd.Timestamp:
    y, m = map(int, ym.split("-"))
    return pd.Timestamp(y, m, 1)

def month_end_str(ym: str) -> pd.Timestamp:
    y, m = map(int, ym.split("-"))
    return (pd.Timestamp(y, m, 1) + MonthEnd(1)).replace(hour=23, minute=59, second=59)

def pick_last_full_months(df: pd.DataFrame, k: int):
    maxd = df["Date"].max()
    last_full = (maxd.replace(day=1) - MonthBegin(1))
    first = (last_full - MonthBegin(k-1)).replace(day=1)
    start = pd.Timestamp(first.year, first.month, 1)
    end   = (last_full + MonthEnd(1)).replace(hour=23, minute=59, second=59)
    return start, end

def canon_category(x: str) -> str:
    if not isinstance(x, str) or not x.strip():
        return "Khác"
    for canon, variants in CATEGORY_CANON.items():
        for v in variants:
            if v.lower() in x.lower():
                return canon
    return x.strip()

# ====== MAIN ======

def main():
    assert os.path.exists(RAW_CSV), f"❌ Không thấy file input: {RAW_CSV}"
    os.makedirs(OUT_DIR, exist_ok=True)

    df = pd.read_csv(RAW_CSV)
    if df.empty:
        print("❌ File rỗng."); return

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"]).copy()

    # Chuẩn hoá Category
    if "Category" in df.columns:
        df["Category"] = df["Category"].apply(canon_category)

    # Khử trùng lặp
    if "Url" in df.columns:
        df = df.drop_duplicates(subset=["Url"], keep="first")
    else:
        df = df.drop_duplicates(subset=["Title", "Date"], keep="first")

    # ==== Lọc theo tháng ====
    if USE_ABSOLUTE_RANGE:
        start = month_start_str(ABS_FROM)
        end   = month_end_str(ABS_TO)
        sel = df[(df["Date"] >= start) & (df["Date"] <= end)].copy()
        note = f"Khoảng tuyệt đối {ABS_FROM} → {ABS_TO}"
    else:
        k = N_LAST_FULL_MONTHS
        while True:
            start, end = pick_last_full_months(df, k)
            sel = df[(df["Date"] >= start) & (df["Date"] <= end)].copy()
            if len(sel) >= MIN_ROWS or k >= 24:
                break
            k += 1
        note = f"{k} tháng nguyên gần nhất (đảm bảo ≥ {MIN_ROWS} bài)"

    # Giữ cột cần thiết
    keep_cols = [c for c in ["Title", "Date", "Category", "Description"] if c in sel.columns]
    sel = sel[keep_cols].sort_values("Date").reset_index(drop=True)

    # Xuất clean data
    sel.to_csv(OUT_CLEAN, index=False, encoding="utf-8-sig")

    # Summary theo tháng và chuyên mục
    tmp = sel.copy()
    tmp["Month"] = tmp["Date"].dt.to_period("M").astype(str)
    by_month = tmp.groupby("Month").size().reset_index(name="Count")
    by_cat = tmp.groupby("Category").size().reset_index(name="Count").sort_values("Count", ascending=False)

    summary = pd.concat([
        by_month.assign(__section__="ByMonth"),
        by_cat.rename(columns={"Category": "Month"}).assign(__section__="ByCategory")
    ])
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")

    print("✅ CLEAN DONE")
    print("🗓️", note, f"→ {start.date()} đến {end.date()}")
    print("📦 clean_data.csv:", len(sel), "dòng  | file:", OUT_CLEAN)
    print("📊 summary.csv   :", len(summary), "dòng  | file:", OUT_SUMMARY)

if __name__ == "__main__":
    main()
