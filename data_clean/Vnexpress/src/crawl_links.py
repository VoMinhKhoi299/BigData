"""
VNEXPRESS PIPELINE (click Run là chạy) — an toàn IP + lọc tháng nguyên
- Output:
  data_raw/vnexpress_links.csv
  data_raw/vnexpress_articles_raw.csv
  data_clean/vnexpress_clean.csv   (>= MIN_ROWS, tháng NGUYÊN liên tiếp)
"""

import os, csv, time, random, json, threading
from datetime import datetime
from typing import List, Tuple, Optional

# ====== CONFIG (chỉnh ngay tại đây) ======
MIN_ROWS       = 1000   # cần tối thiểu
MAX_MONTHS     = 6      # không lấy quá X tháng liền kề (tháng NGUYÊN)
PAGES_PER_CAT  = 30     # số trang/ chuyên mục (giữ vừa để tránh bị chặn)
CONCURRENCY    = 10     # số luồng crawl meta cùng lúc (nhỏ để an toàn)
RPS_LIMIT      = 4      # tối đa ~request/giây (token bucket)
MAX_LINKS_META = 2000   # trần số link đem đi lấy meta (an toàn IP)

CATEGORIES = {
    "Thời sự":   "https://vnexpress.net/thoi-su",
    "Kinh doanh":"https://vnexpress.net/kinh-doanh",
    "Thế giới":  "https://vnexpress.net/the-gioi",
    "Giải trí":  "https://vnexpress.net/giai-tri",
    "Thể thao":  "https://vnexpress.net/the-thao",
    "Pháp luật": "https://vnexpress.net/phap-luat",
    "Giáo dục":  "https://vnexpress.net/giao-duc",
    "Sức khỏe":  "https://vnexpress.net/suc-khoe",
    "Đời sống":  "https://vnexpress.net/doi-song",
    "Du lịch":   "https://vnexpress.net/du-lich",
    "Khoa học":  "https://vnexpress.net/khoa-hoc",
    "Số hóa":    "https://vnexpress.net/so-hoa",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    "Connection": "keep-alive",
}
# =========================================

# === paths
BASE_DIR = os.path.dirname(__file__)
RAW_DIR  = os.path.join(BASE_DIR, "data_raw")
CLEAN_DIR= os.path.join(BASE_DIR, "data_clean")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)
LINKS_CSV = os.path.join(RAW_DIR,  "vnexpress_links.csv")
META_CSV  = os.path.join(RAW_DIR,  "vnexpress_articles_raw.csv")
OUT_CSV   = os.path.join(CLEAN_DIR,"vnexpress_clean.csv")

# === libs
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dparser
import pandas as pd
from pandas.tseries.offsets import MonthBegin, MonthEnd
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# -------------- helpers --------------
def make_session() -> requests.Session:
    s = requests.Session(); s.headers.update(HEADERS)
    retry = Retry(total=5, connect=5, read=5,
                  status_forcelist=[429,500,502,503,504],
                  backoff_factor=1.5, respect_retry_after_header=True)
    adp = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adp); s.mount("http://", adp)
    return s

def fetch_html(url, timeout=20) -> Optional[str]:
    try:
        r = _SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def extract_list_items(html: str, base_url: str) -> List[Tuple[str,str]]:
    soup = BeautifulSoup(html, "lxml")
    items, seen = [], set()
    sels = [
        "article.item-news h3.title-news a",
        "article.item-news h3.title a",
        ".list-news .item-news h3.title-news a",
        "h3.title-news a", "h3.title a", "a.thumb"
    ]
    for sel in sels:
        for a in soup.select(sel):
            href = a.get("href"); 
            if not href: continue
            if href.startswith("/"): href = urljoin(base_url, href)
            if href in seen: continue
            seen.add(href)
            title = a.get_text(strip=True) or a.get("title") or ""
            items.append((title, href))
    return items

def parse_dt(txt: str):
    if not txt: return None
    try: return dparser.parse(txt, dayfirst=True, fuzzy=True)
    except: return None

def extract_meta(html: str, fallback_title: str, fallback_cat: str):
    soup = BeautifulSoup(html, "lxml")
    # Title
    t = soup.select_one("h1.title-detail, h1.title-page")
    title = t.get_text(strip=True) if t else None
    if not title:
        og = soup.select_one('meta[property="og:title"]')
        if og and og.get("content"): title = og["content"].strip()
    if not title: title = fallback_title or ""

    # Date
    dt = None
    el = soup.select_one("span.date, .date")
    if el: dt = parse_dt(el.get_text(" ", strip=True))
    if not dt:
        mp = soup.select_one('meta[property="article:published_time"]')
        if mp and mp.get("content"): dt = parse_dt(mp["content"])
    if not dt:
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(sc.string) if sc.string else None
                objs = data if isinstance(data, list) else [data]
                for obj in objs:
                    if isinstance(obj, dict):
                        v = obj.get("datePublished") or obj.get("dateModified")
                        if v: 
                            dt = parse_dt(v)
                            if dt: break
                if dt: break
            except: pass
    if dt and dt.tzinfo is not None: dt = dt.replace(tzinfo=None)

    # Category
    bc = [a.get_text(strip=True) for a in soup.select("ul.breadcrumb li a")]
    cat = bc[0] if bc else None
    if not cat:
        sec = soup.select_one('meta[property="article:section"]')
        cat = (sec.get("content","").strip() if sec else None)
    if not cat: cat = fallback_cat

    # Description
    d = soup.select_one("p.description, .sidebar-1 p.description")
    if d: desc = d.get_text(" ", strip=True)
    else:
        ogd = soup.select_one('meta[name="description"], meta[property="og:description"]')
        desc = (ogd.get("content","").strip() if ogd else "")

    return title, (dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None), cat, desc

# ---- token bucket limiter to avoid IP block ----
class TokenBucket:
    def __init__(self, rps: float, capacity: Optional[float] = None):
        self.rate = rps
        self.capacity = capacity or rps
        self.tokens = self.capacity
        self.t = time.time()
        self.lock = threading.Lock()
    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                self.tokens = min(self.capacity, self.tokens + (now - self.t)*self.rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.05)

# -------------- STEP 1: crawl links --------------
def crawl_links():
    seen = set()
    if os.path.exists(LINKS_CSV) and os.path.getsize(LINKS_CSV)>0:
        with open(LINKS_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f): seen.add(r["Url"])

    write_header = not os.path.exists(LINKS_CSV) or os.path.getsize(LINKS_CSV)==0
    fout = open(LINKS_CSV, "a", newline="", encoding="utf-8")
    w = csv.writer(fout)
    if write_header: w.writerow(["Title","Url","Category"])

    total_new = 0
    for cat, base in CATEGORIES.items():
        print(f"\n==> [{cat}] {base}")
        fails=0
        for p in range(1, PAGES_PER_CAT+1):
            url = base if p==1 else f"{base}-p{p}"
            html = fetch_html(url, timeout=20)
            if not html:
                fails += 1; print(f"  ⚠️ Trang {p} lỗi (fail={fails})")
                if fails>=3: print("  ⛔ Bỏ qua chuyên mục."); break
                time.sleep(2.5*fails); continue
            fails=0
            items = extract_list_items(html, base)
            print(f"  📄 Trang {p}: {len(items)} link")
            if not items: break
            new_on_page = 0
            for title, href in items:
                if href in seen: continue
                seen.add(href); new_on_page += 1; total_new += 1
                w.writerow([title, href, cat])
            print(f"    + Ghi mới: {new_on_page} | Tổng mới: {total_new}")
            time.sleep(random.uniform(0.6, 1.6))  # nhẹ nhàng để không bị “cặn” IP
    fout.close()
    print(f"\n✅ DONE links → {LINKS_CSV}")

# -------------- STEP 2: crawl meta (safe) --------------
def crawl_meta_safe():
    # read link list
    rows=[]
    with open(LINKS_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        print("❌ Không có link. Hãy chạy crawl_links trước.")
        return

    # cap số link đem đi lấy meta để bảo toàn IP
    if MAX_LINKS_META and len(rows)>MAX_LINKS_META:
        rows = rows[:MAX_LINKS_META]
        print(f"ℹ️ Giới hạn META ở {MAX_LINKS_META} link (để an toàn IP).")

    # resume: skip URL đã có trong META_CSV
    done=set()
    if os.path.exists(META_CSV) and os.path.getsize(META_CSV)>0:
        with open(META_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f): done.add(r["Url"])

    write_header = not os.path.exists(META_CSV) or os.path.getsize(META_CSV)==0
    fout = open(META_CSV, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(fout, fieldnames=["Title","Date","Category","Description","Url"])
    if write_header: w.writeheader()

    # worker pool
    q = Queue()
    for r in rows:
        if r["Url"] in done: continue
        q.put(r)
    for _ in range(CONCURRENCY): q.put(None)

    limiter = TokenBucket(RPS_LIMIT, RPS_LIMIT*2)

    def worker():
        while True:
            item = q.get()
            if item is None: break
            url = item["Url"]; title0=item["Title"]; cat0=item["Category"]
            limiter.wait()
            html = fetch_html(url, timeout=15)
            if html:
                title, date, cat, desc = extract_meta(html, title0, cat0)
                w.writerow({"Title":title,"Date":date,"Category":cat,"Description":desc,"Url":url})
            q.task_done()

    threads=[]
    for _ in range(CONCURRENCY):
        t = threading.Thread(target=worker, daemon=True)
        threads.append(t); t.start()
    q.join()
    for t in threads: t.join()
    fout.close()
    print(f"✅ DONE meta → {META_CSV}")

# -------------- STEP 3: filter whole months until >= MIN_ROWS --------------
def filter_full_months_min_rows():
    if not (os.path.exists(META_CSV) and os.path.getsize(META_CSV)>0):
        print("❌ Chưa có meta CSV.")
        return
    df = pd.read_csv(META_CSV, parse_dates=["Date"])
    df = df.dropna(subset=["Date"]).copy()
    if df.empty:
        print("❌ Không có dòng Date hợp lệ.")
        return

    maxd = df["Date"].max()
    last_full = (maxd.replace(day=1) - MonthBegin(1))

    months = []
    for k in range(MAX_MONTHS):
        m_start = (last_full - MonthBegin(k)).replace(day=1)
        m_end   = (m_start + MonthEnd(1)).replace(hour=23, minute=59, second=59)
        months.append((m_start, m_end))

    parts=[]; total=0
    for s,e in months:
        part = df[(df["Date"]>=s) & (df["Date"]<=e)].copy()
        parts.append(part); total += len(part)
        if total >= MIN_ROWS: break

    out = pd.concat(parts).sort_values("Date")
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"🗓️ Khoảng: {out['Date'].min().date()} → {out['Date'].max().date()} | rows={len(out)}")
    print(f"✅ Saved clean → {OUT_CSV}")

# -------------- MAIN --------------
def main():
    print("▶︎ Bắt đầu PIPELINE (giới hạn an toàn, lọc tháng nguyên)…")
    crawl_links()
    crawl_meta_safe()
    filter_full_months_min_rows()
    print("🎉 HOÀN TẤT.")

if __name__ == "__main__":
    _SESSION = make_session()
    main()
