#!/usr/bin/env python3
# Python 3.12
# VNExpress: crawl theo THÁNG NGUYÊN bằng sitemap tháng -> gom link (có giới hạn) -> hydrate chi tiết
import os, csv, time, random, re, hashlib, datetime as dt
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup

# ======== CẤU HÌNH =========
YEAR   = 2025
MONTHS = [7, 8, 9]            # THÁNG NGUYÊN (mặc định 07–09/2025). Đổi tùy ý.
MAX_PER_MONTH = 220           # giới hạn link mỗi tháng để tổng ~600-700
HARD_CAP_TOTAL = 1200         # tránh crawl quá nhiều
BASE_DIR = os.path.abspath("./data/raw")
os.makedirs(BASE_DIR, exist_ok=True)
OUTFILE = os.path.join(BASE_DIR, "vnexpress_2025Q3_raw.csv")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
S = requests.Session()
S.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8"
})
TIMEOUT, RETRIES = 18, 2

def sleep(a=0.55,b=0.65): time.sleep(a + random.random()*b)

def get(url, **kw):
    for i in range(RETRIES+1):
        try:
            r = S.get(url, timeout=TIMEOUT, **kw)
            if r.status_code in (200,301,302): return r
            if r.status_code in (429,403): time.sleep(10 + random.random()*10)
        except requests.RequestException:
            pass
        time.sleep(1.1 + i*1.2)
    return None

def in_month_range(d: dt.datetime) -> bool:
    return d.year == YEAR and d.month in MONTHS

def sitemap_month_urls():
    r = get("https://vnexpress.net/sitemap.xml")
    if not r: return []
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    patt = [f"{YEAR}-{m:02d}" for m in MONTHS]
    out = []
    for loc in root.findall(".//sm:loc", ns):
        u = (loc.text or "").strip()
        if any(p in u for p in patt):
            out.append(u)
    return sorted(out)

def links_from_month_sitemap(sm_url, month):
    r = get(sm_url)
    if not r: return []
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall(".//sm:loc", ns):
        u = (loc.text or "").strip()
        if u: urls.append(u)
    random.shuffle(urls)
    # cắt bớt ngay tại đây để không hydrate quá nhiều
    return urls[:MAX_PER_MONTH]

def strip_html(x):
    return BeautifulSoup(x or "", "html.parser").get_text(" ").strip()

def parse_article(url):
    r = get(url)
    if not r: return None
    soup = BeautifulSoup(r.text, "html.parser")
    def meta(prop=None, name=None):
        tag = soup.find("meta", attrs={"property":prop}) if prop else soup.find("meta", attrs={"name":name})
        return (tag.get("content") or "").strip() if tag and tag.has_attr("content") else ""
    title = meta(prop="og:title")
    desc  = meta(name="description")
    cate  = meta(prop="article:section")
    pub   = meta(prop="article:published_time")
    pub_dt = None
    if pub:
        try: pub_dt = dt.datetime.fromisoformat(re.sub("Z$","+00:00", pub))
        except: pass
    if not pub_dt:
        t = soup.find("time")
        raw = (t.get("datetime") or t.get_text(strip=True)) if t else ""
        if raw:
            try: pub_dt = dt.datetime.fromisoformat(re.sub("Z$","+00:00", raw))
            except: pass
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": strip_html(title),
        "date": pub_dt.isoformat() if pub_dt else "",
        "category": strip_html(cate),
        "description": strip_html(desc),
        "link": url
    }

def run():
    print(f"[VNE] Tháng nguyên: {', '.join(f'{m:02d}/{YEAR}' for m in MONTHS)}")
    month_maps = sitemap_month_urls()
    # map: month -> list url (giới hạn MAX_PER_MONTH)
    chosen_urls = []
    for sm in month_maps:
        m = int(sm.split("-")[-1].split(".")[0])  # .../2025-07.xml
        if m not in MONTHS: continue
        urls = links_from_month_sitemap(sm, m)
        print(f"[VNE] {sm} -> lấy {len(urls)} link")
        chosen_urls.extend(urls)
        if len(chosen_urls) >= HARD_CAP_TOTAL: break

    rows = []
    for i, url in enumerate(chosen_urls, 1):
        art = parse_article(url)
        if not art or not art["date"]:
            sleep(); continue
        try:
            d = dt.datetime.fromisoformat(art["date"])
        except:
            sleep(); continue
        if not in_month_range(d):
            continue
        rows.append(art)
        if i % 40 == 0:
            print(f"[VNE] hydrated {i}/{len(chosen_urls)} -> kept {len(rows)}")
        sleep(0.35,0.55)

    rows.sort(key=lambda x: x["date"])
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","title","date","category","description","link"])
        w.writeheader(); w.writerows(rows)
    print(f"[VNE] DONE → {OUTFILE} (records={len(rows)})")

if __name__ == "__main__":
    run()
