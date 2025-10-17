import os, json, time, datetime as dt, random
import requests, pandas as pd
from dotenv import load_dotenv

AUTH_BASE = "https://charts-spotify-com-service.spotify.com/auth/v0/charts"
UA = "Mozilla/5.0"
CSV_OUT = "data_clean/Spotify/data_raw/spotify_viral_global_vn.csv"

def load_session():
    load_dotenv()
    token = os.getenv("SPOTIFY_BEARER", "").strip()
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "authorization": f"Bearer {token}"})
    return s

def fetch_json(s, url):
    r = s.get(url, timeout=30)
    if r.status_code == 200:
        return r.json()
    return None

def parse_entries(data, region, day):
    rows = []
    for e in data.get("entries", []):
        tm = e.get("trackMetadata", {})
        ce = e.get("chartEntryData", {})
        
        names = [a["name"] for a in tm.get("artists", []) if a.get("name")]
        
        rows.append({
            "chart_type": "viral_daily",
            "period": "daily",
            "date": day,
            "region": region.lower(),
            "rank": ce.get("currentRank"),
            "previous_rank": ce.get("previousRank"),
            "track_name": tm.get("trackName"),
            "artists": ", ".join(names) if names else None,
            "release_date": tm.get("releaseDate"),
        })
    return rows

def crawl(regions, start, end):
    s = load_session()
    start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
    end_date = dt.datetime.strptime(end, "%Y-%m-%d").date()
    
    all_rows = []
    current = start_date
    while current <= end_date:
        day = current.strftime("%Y-%m-%d")
        for region in regions:
            url = f"{AUTH_BASE}/viral-{region.lower()}-daily/{day}"
            data = fetch_json(s, url)
            if data:
                all_rows.extend(parse_entries(data, region, day))
        
        current += dt.timedelta(days=1)
        time.sleep(random.uniform(0.3, 0.7))  # Delay giữa các request
    
    # Xuất CSV
    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    print(f"Crawl completed: {len(df)} records")

def main():
    crawl(
        regions=["global", "vn"], 
        start="2025-01-01", 
        end="2025-01-31"
    )

if __name__ == "__main__":
    main()