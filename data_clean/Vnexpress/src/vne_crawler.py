#!/usr/bin/env python3
# Python 3.12 – Crawl VNExpress theo tháng nguyên (sitemap)
import os, csv, re, time, random, hashlib, datetime as dt, xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup

YEAR, MONTHS = 2025, [7, 8, 9]
MAX_PER_MONTH, HARD_CAP = 220, 1200
OUT = "data/raw/vnexpress_2025Q3_raw.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})

def get(url):
    for _ in range(3):
        try:
            r = S.get(url, timeout=15)
            if r.ok: return r
        except: pass
        time.sleep(1)
    return None

def sitemap_months():
    r = get("https://vnexpress.net/sitemap.xml")
    if not r: return []
    root = ET.fromstring(r.content)
    patt = [f"{YEAR}-{m:02d}" for m in MONTHS]
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [loc.text.strip() for loc in root.findall(".//sm:loc", ns)
            if any(p in (loc.text or "") for p in patt)]

def month_links(sm):
    r = get(sm)
    if not r: return []
    root = ET.fromstring(r.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    links = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
    random.shuffle(links)
    return links[:MAX_PER_MONTH]

def text(x): return BeautifulSoup(x or "", "html.parser").get_text(" ").strip()

def parse_article(u):
    r = get(u)
    if not r: return None
    s = BeautifulSoup(r.text, "html.parser")
    def meta(p): 
        t = s.find("meta", {"property": p}) or s.find("meta", {"name": p})
        return t["content"].strip() if t and t.has_attr("content") else ""
    title, desc = meta("og:title"), meta("description")
    cate, pub = meta("article:section"), meta("article:published_time")
    d = None
    if pub:
        try: d = dt.datetime.fromisoformat(re.sub("Z$","+00:00", pub))
        except: pass
    if not d:
        t = s.find("time")
        if t:
            raw = t.get("datetime") or t.get_text(strip=True)
            try: d = dt.datetime.fromisoformat(re.sub("Z$","+00:00", raw))
            except: pass
    return {"id": hashlib.md5(u.encode()).hexdigest(),
            "title": text(title), "date": d.isoformat() if d else "",
            "category": text(cate), "description": text(desc), "link": u}

def run():
    maps = sitemap_months()
    all_links, seen = [], set()
    for sm in maps:
        try: m = int(sm.split("-")[-1].split(".")[0])
        except: continue
        if m not in MONTHS: continue
        for u in month_links(sm):
            if u not in seen:
                seen.add(u)
                all_links.append(u)
        if len(all_links) >= HARD_CAP: break

    rows = []
    for i, u in enumerate(all_links, 1):
        a = parse_article(u)
        if not a or not a["date"]: continue
        try: d = dt.datetime.fromisoformat(a["date"])
        except: continue
        if d.year == YEAR and d.month in MONTHS:
            rows.append(a)
        if i % 40 == 0:
            print(f"{i}/{len(all_links)} -> {len(rows)}")
        time.sleep(random.uniform(0.3, 0.5))

    rows.sort(key=lambda x: x["date"])
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","title","date","category","description","link"])
        w.writeheader(); w.writerows(rows)
    print("DONE:", len(rows), "->", OUT)

if __name__ == "__main__":
    run()
