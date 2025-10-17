#!/usr/bin/env python3
# Clean & chuẩn hoá Spotify Viral Daily (global+vn) -> 10 cột cố định
# In : data_raw/spotify_viral_global_vn.csv
# Out: data_clean/clean_data.csv

import os, argparse
import pandas as pd, numpy as np

IN_DEFAULT  = "data_raw/spotify_viral_global_vn.csv"
OUT_DEFAULT = "data_clean/clean_data.csv"
DROP_COLS   = {"period","track_id","weeks_on_chart","week_on_chart","artist_uris","retrieved_at_utc","date_iso"}
OUT_COLS    = ["date","region","chart_type","rank","previous_rank","rank_delta","movement","track_name","artists","release_date"]

def remove_quotes(text):
    """Bỏ tất cả nháy kép và xuống dòng, tab."""
    if pd.isna(text):
        return ""
    return str(text).replace('"', '').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()

def main():
    ap = argparse.ArgumentParser(description="ETL clean Spotify Viral Daily -> 10 cols")
    ap.add_argument("--inp", default=IN_DEFAULT); ap.add_argument("--out", default=OUT_DEFAULT)
    args = ap.parse_args(); os.makedirs(os.path.dirname(args.out), exist_ok=True)

    peek = pd.read_csv(args.inp, nrows=5)
    parse_dates = ["date"] + (["retrieved_at_utc"] if "retrieved_at_utc" in peek.columns else [])
    df = pd.read_csv(args.inp, parse_dates=parse_dates, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]

    # Đảm bảo cột cần thiết
    need = ["chart_type","date","region","rank","previous_rank","track_name","artists","release_date"]
    for c in need:
        if c not in df.columns: df[c] = pd.NA

    # Chuẩn hóa text
    for c in ["chart_type","region","track_name","artists","release_date"]:
        df[c] = df[c].astype("string").apply(remove_quotes)

    df["region"] = df["region"].str.lower()
    df["chart_type"] = df["chart_type"].str.lower().fillna("viral_daily")
    df = df[df["chart_type"] == "viral_daily"].copy()

    # Dữ liệu số và ngày
    for c in ["rank","previous_rank"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Dedupe
    key = ["chart_type","region","date","track_name","artists"]
    df = df.sort_values(key+["rank"]).drop_duplicates(subset=key, keep="first")

    # Feature
    df["rank_delta"] = df["previous_rank"].astype(float) - df["rank"].astype(float)
    df["movement"]   = np.where(df["previous_rank"].isna(), "NEW",
                        np.where(df["rank_delta"]>0, "UP",
                        np.where(df["rank_delta"]<0, "DOWN", "SAME")))

    # Giữ 10 cột
    df = df.reindex(columns=OUT_COLS).sort_values(["date","region","rank"])

    # 🔹 Ghi thủ công, không escape, không quote
    with open(args.out, "w", encoding="utf-8-sig") as f:
        f.write(",".join(OUT_COLS) + "\n")
        for _, row in df.iterrows():
            values = ["" if pd.isna(v) else str(v) for v in row]
            f.write(",".join(values) + "\n")

    print("✅ Wrote:", args.out)
    print("   Rows:", len(df))
    if len(df): print("   Date range:", df["date"].min().date(), "->", df["date"].max().date())

if __name__ == "__main__":
    main()
