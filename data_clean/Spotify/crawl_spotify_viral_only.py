# - Dùng token trong .env (SPOTIFY_BEARER)
# - Hỗ trợ resume: nếu raw_json đã có thì không gọi lại
# - Tôn trọng RPS + jitter đơn giản
# - Ghi ra CSV data_raw/spotify_viral_global_vn.csv với schema cố định

import os, json, time, argparse, datetime as dt, random
from pathlib import Path
from typing import List, Dict, Any
import requests, pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# ----------- Cấu hình cơ bản -----------
AUTH_BASE = "https://charts-spotify-com-service.spotify.com/auth/v0/charts"
UA        = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15"

RAW_DIR   = Path("data_raw/raw_json")
CSV_OUT   = Path("data_raw/spotify_viral_global_vn.csv")
LOG_FILE  = Path("logs/crawl.log")

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# ----------- Tiện ích ngắn gọn -----------
def log(msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def load_session() -> requests.Session:
    load_dotenv()
    token = os.getenv("SPOTIFY_BEARER", "").strip()
    if not token:
        raise SystemExit("Thiếu SPOTIFY_BEARER trong .env")
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "authorization": f"Bearer {token}"})
    return s

def sleep_rps(rps: float, jitter: float = 0.35):
    base = 1.0 / max(rps, 0.01)
    time.sleep(max(0.0, base + random.uniform(-base*jitter, base*jitter)))

def fetch_json(s: requests.Session, url: str, raw_path: Path, resume: bool) -> Dict[str, Any]:
    """Đọc cache nếu có; gọi mạng đơn giản với xử lý 401/404/429."""
    if resume and raw_path.exists():
        with open(raw_path, "r", encoding="utf-8") as f:
            return json.load(f)

    r = s.get(url, timeout=30)
    if r.status_code == 401:
        raise SystemExit("401 Unauthorized — refresh token trong .env rồi chạy lại với --resume")
    if r.status_code == 404:
        log(f"404 {url}")
        return {"__status": 404}
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "5"))
        log(f" 429 {url} — đợi {wait}s")
        time.sleep(wait)
        r = s.get(url, timeout=30)  # thử lại 1 lần
    r.raise_for_status()

    data = r.json()
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return data

def parse_entries(data: Dict[str, Any], region: str, day: str) -> List[Dict[str, Any]]:
    """Rút gọn: chỉ giữ các trường phục vụ ETL/Thống kê; KHÔNG có week_*."""
    rows = []
    entries = data.get("entries") or []
    retrieved_at = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for e in entries:
        tm = e.get("trackMetadata") or {}
        ce = e.get("chartEntryData") or {}
        if not tm:  # bỏ entry không phải bài hát
            continue

        # ghép danh sách nghệ sĩ
        names, uris = [], []
        for a in (tm.get("artists") or []):
            if a.get("name"): names.append(a["name"])
            u = a.get("spotifyUri") or a.get("uri")
            if u: uris.append(u)

        # lấy track_id từ URI nếu có (spotify:track:<id>)
        track_uri = tm.get("trackUri") or tm.get("uri")
        track_id = None
        if track_uri:
            parts = track_uri.split(":")
            if len(parts) >= 3 and parts[-2] == "track": track_id = parts[-1]

        rows.append({
            # định danh bản ghi
            "chart_type": "viral_daily",
            "period": "daily",
            "date": day,
            "region": region.lower(),
            "retrieved_at_utc": retrieved_at,

            # xếp hạng
            "rank": ce.get("currentRank"),
            "previous_rank": ce.get("previousRank"),
            "weeks_on_chart": ce.get("weeksOnChart"),

            # mô tả bài hát
            "track_name": tm.get("trackName"),
            "artists": ", ".join(names) if names else None,
            "artist_uris": ";".join(uris) if uris else None,
            "track_id": track_id,
            "release_date": tm.get("releaseDate"),
        })
    return rows

def write_csv(rows: List[Dict[str, Any]]):
    if not rows: return
    df = pd.DataFrame(rows)
    cols = ["chart_type","period","date","region","retrieved_at_utc",
            "rank","previous_rank","weeks_on_chart",
            "track_name","artists","artist_uris","track_id","release_date"]
    df = df.reindex(columns=cols)
    # append có/không header tuỳ file tồn tại
    df.to_csv(CSV_OUT, mode="a" if CSV_OUT.exists() else "w",
              header=not CSV_OUT.exists(), index=False, encoding="utf-8-sig")

def each_day(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur.strftime("%Y-%m-%d")
        cur += dt.timedelta(days=1)

# ----------- Chương trình chính -----------
def crawl(regions: List[str], start: str, end: str, rps: float, resume: bool):
    s = load_session()
    days = list(each_day(dt.datetime.strptime(start, "%Y-%m-%d").date(),
                         dt.datetime.strptime(end,   "%Y-%m-%d").date()))
    tasks = [(d, r) for d in days for r in regions]
    pbar = tqdm(total=len(tasks), desc=" Viral daily")

    for day, region in tasks:
        url = f"{AUTH_BASE}/viral-{region.lower()}-daily/{day}"
        raw = RAW_DIR / f"{day}_{region.lower()}_viral_daily.json"
        try:
            data = fetch_json(s, url, raw, resume=resume)
            if data.get("__status") != 404:
                write_csv(parse_entries(data, region, day))
        except Exception as ex:
            log(f"ERR {url}: {ex}")
        finally:
            pbar.update(1)
            sleep_rps(rps)
    pbar.close()

def main():
    ap = argparse.ArgumentParser(description="Crawl Spotify VIRAL daily -> CSV")
    ap.add_argument("--regions", nargs="+", default=["global","vn"])   # chọn khu vực
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")       # ngày bắt đầu
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD")       # ngày kết thúc
    ap.add_argument("--rps", type=float, default=0.30)                 # requests/second
    ap.add_argument("--resume", action="store_true")                   # bỏ qua URL đã có raw JSON
    args = ap.parse_args()

    print(f" REGIONS: {args.regions}")
    print(f" DATE RANGE: {args.start} -> {args.end}")
    print(f" RPS={args.rps} | RESUME={args.resume}")
    crawl(args.regions, args.start, args.end, args.rps, args.resume)

if __name__ == "__main__":
    main()
