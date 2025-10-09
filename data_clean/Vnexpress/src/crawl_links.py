import os, csv, time, random, json, threading
from datetime import datetime
import requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dateutil import parser as dparser
from pandas.tseries.offsets import MonthBegin, MonthEnd
from concurrent.futures import ThreadPoolExecutor

# ===== CONFIG =====
MIN_ROWS, MAX_MONTHS = 1000, 6
PAGES_PER_CAT, CONCURRENCY, RPS_LIMIT = 30, 10, 4
MAX_LINKS_META = 2000
CATEGORIES = {
    "Thời sự": "https://vnexpress.net/thoi-su",
    "Kinh doanh": "https://vnexpress.net/kinh-doanh",
    "Thế giới": "https://vnexpress.net/the-gioi",
    "Giải trí": "https://vnexpress.net/giai-tri",
    "Thể thao": "https://vnexpress.net/the-thao",
    "Pháp luật": "https://vnexpress.net/phap-luat",
    "Giáo dục": "https://vnexpress.net/giao-duc",
    "Sức khỏe": "https://vnexpress.net/suc-khoe",
    "Đời sống": "https://vnexpress.net/doi-song",
    "Du lịch": "https://vnexpress.net/du-lich",
    "Khoa học": "https://vnexpress.net/khoa-hoc",
    "Số hóa": "https://vnexpress.net/so-hoa",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}

BASE = os.path.dirname(__file__)
RAW, CLEAN = os.path.join(BASE,"data_raw"), os.path.join(BASE,"data_clean")
os.makedirs(RAW,exist_ok=True), os.makedirs(CLEAN,exist_ok=True)
LINKS, META, OUT = [os.path.join(RAW,n) for n in ["vnexpress_links.csv","vnexpress_articles_raw.csv"]], os.path.join(CLEAN,"vnexpress_clean.csv")

# ===== UTILS =====
s = requests.Session(); s.headers.update(HEADERS)
def get_html(u): 
    try: r=s.get(u,timeout=20); r.raise_for_status(); return r.text
    except: return None

def parse_dt(t):
    try: return dparser.parse(t,dayfirst=True,fuzzy=True)
    except: return None

def extract_links(html, base):
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.select("h3.title-news a"):
        href = urljoin(base,a["href"]); urls.append((a.get_text(strip=True),href))
    return urls

def extract_meta(html,t0,c0):
    s = BeautifulSoup(html,"lxml")
    title = (s.select_one("h1.title-detail") or s.select_one('meta[property="og:title"]'))
    title = title.get_text(strip=True) if title and title.name=="h1" else (title["content"] if title else t0)
    date = parse_dt((s.select_one(".date") or {}).get_text(" ",strip=True) if s.select_one(".date") else "")
    if not date:
        m=s.select_one('meta[property="article:published_time"]')
        date=parse_dt(m["content"]) if m else None
    cat = (s.select_one("ul.breadcrumb li a") or {}).get_text(strip=True) if s.select_one("ul.breadcrumb li a") else c0
    desc = (s.select_one("p.description") or {}).get_text(" ",strip=True) if s.select_one("p.description") else ""
    return title,date.strftime("%Y-%m-%d %H:%M:%S") if date else "",cat,desc

# ===== STEP 1: LINK =====
def crawl_links():
    seen=set()
    if os.path.exists(LINKS[0]):
        seen={r["Url"] for r in csv.DictReader(open(LINKS[0],encoding="utf-8"))}
    f=open(LINKS[0],"w",newline="",encoding="utf-8")
    w=csv.writer(f); w.writerow(["Title","Url","Category"])
    for cat,base in CATEGORIES.items():
        for p in range(1,PAGES_PER_CAT+1):
            html=get_html(base if p==1 else f"{base}-p{p}")
            if not html: break
            for t,u in extract_links(html,base):
                if u not in seen: seen.add(u); w.writerow([t,u,cat])
            time.sleep(random.uniform(0.6,1.5))
    f.close()

# ===== STEP 2: META =====
def crawl_meta():
    rows=list(csv.DictReader(open(LINKS[0],encoding="utf-8")))
    rows=rows[:MAX_LINKS_META]
    done=set()
    if os.path.exists(LINKS[1]):
        done={r["Url"] for r in csv.DictReader(open(LINKS[1],encoding="utf-8"))}
    fout=open(LINKS[1],"a",newline="",encoding="utf-8")
    w=csv.DictWriter(fout,fieldnames=["Title","Date","Category","Description","Url"])
    if not done: w.writeheader()
    lock=threading.Lock()
    def worker(r):
        if r["Url"] in done: return
        html=get_html(r["Url"]); 
        if not html: return
        t,d,c,desc=extract_meta(html,r["Title"],r["Category"])
        with lock: w.writerow({"Title":t,"Date":d,"Category":c,"Description":desc,"Url":r["Url"]})
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        ex.map(worker, rows)
    fout.close()

# ===== STEP 3: FILTER =====
def clean_months():
    df=pd.read_csv(LINKS[1],parse_dates=["Date"]).dropna(subset=["Date"])
    if df.empty: return
    maxd=df["Date"].max()
    last=(maxd.replace(day=1)-MonthBegin(1))
    parts=[]; total=0
    for k in range(MAX_MONTHS):
        s=(last-MonthBegin(k)).replace(day=1)
        e=(s+MonthEnd(1)).replace(hour=23,minute=59,second=59)
        part=df[(df["Date"]>=s)&(df["Date"]<=e)]
        parts.append(part); total+=len(part)
        if total>=MIN_ROWS: break
    pd.concat(parts).sort_values("Date").to_csv(OUT,index=False,encoding="utf-8-sig")

def main():
    crawl_links()
    crawl_meta()
    clean_months()

if __name__=="__main__":
    main()
