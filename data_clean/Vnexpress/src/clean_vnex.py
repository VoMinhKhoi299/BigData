import os, pandas as pd
from pandas.tseries.offsets import MonthBegin, MonthEnd

# ===== CONFIG =====
BASE = os.path.dirname(__file__)
RAW  = os.path.join(BASE, "data_raw", "vnexpress_articles_raw.csv")
OUTD = os.path.join(BASE, "data_clean")
OUTC = os.path.join(OUTD, "clean_data.csv")
OUTS = os.path.join(OUTD, "summary.csv")

USE_ABS = False
ABS_FROM, ABS_TO = "2025-01", "2025-06"
N_LAST, MIN_ROWS = 3, 1000

CAT_CANON = {
    "Thời sự": ["Thời sự", "Thoi su"],
    "Kinh doanh": ["Kinh doanh", "Kinh tế"],
    "Thế giới": ["Thế giới", "The gioi"],
    "Giải trí": ["Giải trí", "Giai tri"],
    "Thể thao": ["Thể thao", "The thao"],
}

def canon_cat(x):
    if not isinstance(x, str) or not x.strip(): return "Khác"
    for k,v in CAT_CANON.items():
        for t in v:
            if t.lower() in x.lower(): return k
    return x.strip()

def pick_last_months(df,k):
    maxd = df["Date"].max()
    last = (maxd.replace(day=1)-MonthBegin(1))
    start = (last - MonthBegin(k-1)).replace(day=1)
    end = (last + MonthEnd(1)).replace(hour=23,minute=59,second=59)
    return start,end

def main():
    assert os.path.exists(RAW), f"Không thấy {RAW}"
    os.makedirs(OUTD, exist_ok=True)

    df = pd.read_csv(RAW)
    if df.empty: return
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=["Date"])
    if "Category" in df.columns: df["Category"] = df["Category"].apply(canon_cat)
    if "Url" in df.columns: df = df.drop_duplicates(subset=["Url"])
    else: df = df.drop_duplicates(subset=["Title","Date"])

    if USE_ABS:
        y1,m1 = map(int,ABS_FROM.split("-")); y2,m2 = map(int,ABS_TO.split("-"))
        start = pd.Timestamp(y1,m1,1)
        end = (pd.Timestamp(y2,m2,1)+MonthEnd(1)).replace(hour=23,minute=59,second=59)
        sel = df[(df["Date"]>=start)&(df["Date"]<=end)].copy()
    else:
        k=N_LAST
        while True:
            start,end = pick_last_months(df,k)
            sel = df[(df["Date"]>=start)&(df["Date"]<=end)]
            if len(sel)>=MIN_ROWS or k>=24: break
            k+=1

    keep=[c for c in ["Title","Date","Category","Description"] if c in sel.columns]
    sel=sel[keep].sort_values("Date").reset_index(drop=True)
    sel.to_csv(OUTC,index=False,encoding="utf-8-sig")

    tmp=sel.copy()
    tmp["Month"]=tmp["Date"].dt.to_period("M").astype(str)
    by_month=tmp.groupby("Month").size().reset_index(name="Count")
    by_cat=tmp.groupby("Category").size().reset_index(name="Count").sort_values("Count",ascending=False)
    summary=pd.concat([by_month.assign(_="ByMonth"),by_cat.rename(columns={"Category":"Month"}).assign(_="ByCategory")])
    summary.to_csv(OUTS,index=False,encoding="utf-8-sig")

    print("CLEAN DONE")
    print(f"{start.date()} → {end.date()} | clean={len(sel)} | summary={len(summary)}")

if __name__ == "__main__":
    main()
