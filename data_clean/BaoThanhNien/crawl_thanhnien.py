#!/usr/bin/env python3
# Python 3.12 – Crawl ThanhNien.vn bằng RSS (đầy đủ & tương đương VNExpress)
import os, csv, time, random, hashlib
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# ===== CONFIG =====
MONTHS_BACK = 3
MAX_PER_FEED = 300
BASE_DIR = os.path.abspath("./data_raw")
os.makedirs(BASE_DIR, exist_ok=True)
OUTFILE = os.path.join(BASE_DIR, "thanhnien_articles_raw.csv")

RSS_FEEDS = {
    "Thời sự":   "https://thanhnien.vn/rss/thoi-su.rss",
    "Thế giới":  "https://thanhnien.vn/rss/the-gioi.rss",
    "Kinh tế":   "https://thanhnien.vn/rss/kinh-te.rss",
    "Giải trí":  "https://thanhnien.vn/rss/giai-tri.rss",
    "Thể thao":  "https://thanhnien.vn/rss/the-thao.rss",
    "Giáo dục":  "https://thanhnien.vn/rss/giao-duc.rss",
    "Sức khỏe":  "https://thanhnien.vn/rss/suc-khoe.rss",
    "Đời sống":  "https://thanhnien.vn/rss/doi-song.rss",
    "Văn hóa":   "https://thanhnien.vn/rss/van-hoa.rss",
    "Du lịch":   "https://thanhnien.vn/rss/du-lich.rss",
    "Công nghệ": "https://thanhnien.vn/rss/cong-nghe.rss",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}
S = requests.Session(); S.headers.update(HEADERS)
TIMEOUT = 15

# ===== HỖ TRỢ =====
def get(url):
    for _ in range(3):
        try:
            r = S.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
        except:
            pass
        time.sleep(random.uniform(1,2))
    return None

def parse_pub_date(pub):
    """Fix năm 2 chữ số -> 4 chữ số"""
    if not pub: return None
    try:
        # Thử dạng chuẩn (có 4 chữ số)
        return datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
    except:
        try:
            # Fix dạng năm 2 chữ số (vd: 'Thu, 16 Oct 25 16:39:00 +0700')
            pub_fixed = pub.replace(" 25 ", " 2025 ")
            return datetime.strptime(pub_fixed, "%a, %d %b %Y %H:%M:%S %z")
        except:
            return None

def parse_rss(url):
    xml = get(url)
    if not xml: return []
    root = ET.fromstring(xml)
    items = []
    for it in root.findall(".//item"):
        title = it.findtext("title", "").strip()
        link = it.findtext("link", "").strip()
        desc = it.findtext("description", "").strip()
        pub = it.findtext("pubDate", "")
        d = parse_pub_date(pub)
        items.append({"Title": title, "Url": link, "Description": desc, "Date": d})
    return items[:MAX_PER_FEED]

def parse_article(url, fallback_title, fallback_cat, desc):
    html = get(url)
    if not html: return None
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one("h1.details__headline")
    title = title.get_text(strip=True) if title else fallback_title

    cat = soup.select_one('meta[property="article:section"]')
    category = cat.get("content", "").strip() if cat else fallback_cat

    tag = soup.select_one('meta[property="article:published_time"]')
    date = ""
    if tag and tag.get("content"):
        date = tag["content"].split("+")[0].strip()

    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "Title": title,
        "Date": date,
        "Category": category,
        "Description": desc,
        "Url": url
    }

# ===== CHÍNH =====
def main():
    print("Bắt đầu crawl ThanhNien.vn (RSS)...")
    all_items = []
    for cat, rss in RSS_FEEDS.items():
        print(f"==> {cat}")
        items = parse_rss(rss)
        for it in items:
            it["Category"] = cat
        print(f"  Lấy {len(items)} bài từ {rss}")
        all_items.extend(items)
        time.sleep(random.uniform(1,2))

    cutoff = datetime.now(timezone.utc) - timedelta(days=MONTHS_BACK*30)
    rows = []
    for i, it in enumerate(all_items, 1):
        d = it["Date"]
        if not d or d < cutoff:
            continue
        art = parse_article(it["Url"], it["Title"], it["Category"], it["Description"])
        if art:
            rows.append(art)
        if i % 50 == 0:
            print(f"  Đã xử lý {i}/{len(all_items)} link...")
        time.sleep(random.uniform(0.4,0.8))

    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","Title","Date","Category","Description","Url"])
        w.writeheader(); w.writerows(rows)

    print(f"✅ Hoàn tất: {len(rows)} bài viết → {OUTFILE}")

if __name__ == "__main__":
    main()
