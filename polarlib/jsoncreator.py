import requests
from bs4 import BeautifulSoup
import json
import os
import hashlib
from urllib.parse import urlparse
import re

def generate_uid(url):

    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "")
    path = parsed.path.strip("/").replace("/", "-")
    
    if path == "":
        path = hashlib.md5(url.encode()).hexdigest()[:12]
    
    return f"{domain}-{path}"

def extract_text(soup):

    paragraphs = soup.find_all(["p"])
    text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
    return text

def extract_images(soup):

    imgs = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if src and src.startswith("http"):
            imgs.append(src)
    return list(dict.fromkeys(imgs))

def normalize_date(date_str):
    if not date_str:
        return ""

    date_str = date_str.split("T")[0].split(" ")[0]
    
    date_str = date_str.replace("/", "-").replace(".", "-")

   
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    return ""

def extract_publication_date(soup):
    meta_selectors = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publish-date"}),
        ("meta", {"name": "Publication-date"}),
        ("meta", {"name": "date"}),
        ("meta", {"property": "og:updated_time"}),
        ("meta", {"itemprop": "datePublished"}),
        ("meta", {"itemprop": "dateModified"}),
    ]

    for tag, attrs in meta_selectors:
        meta = soup.find(tag, attrs)
        if meta and meta.get("content"):
            return normalize_date(meta["content"])
    
    time_tag = soup.find("time")
    if time_tag:
       
        if time_tag.get("datetime"):
            return normalize_date(time_tag["datetime"])
       
        txt = time_tag.get_text(strip=True)
        if txt:
            return normalize_date(txt)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)

           
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        if "datePublished" in item:
                            return normalize_date(item["datePublished"])
            
            if "datePublished" in data:
                 return normalize_date(data["datePublished"])
        except Exception:
            pass

    text = soup.get_text(" ", strip=True)
    match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if match:
        return normalize_date(match.group(1))

   
    return ""


def scrape_article(url):

    print(f"[+] Fetching: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    title = soup.find("title").get_text(strip=True) if soup.find("title") else ""

    images = extract_images(soup)
    text = extract_text(soup)
    publication_date = extract_publication_date(soup)
    uid = generate_uid(url)
    
    domain = urlparse(url).netloc
    lang = soup.html.get("lang") if soup.html else ""
    author_tag = soup.find("meta", {"name": "author"})
    author = author_tag.get("content") if author_tag else ""
    desc_tag = soup.find("meta", {"name": "description"})
    description = desc_tag.get("content") if desc_tag else ""
    keywords_tag = soup.find("meta", {"name": "keywords"})
    keywords = keywords_tag.get("content") if keywords_tag else ""

    return {
        "url": url,
        "uid": uid,
        "images": images,
        "publication-date": publication_date,
        "text": text,
        "title": title,
        "top-image": images[0] if images else "",
        "domain": domain,
        "lang": lang,
        "author": author,
        "description": description,
        "keywords": keywords
    }


def scrape_multiple(urls, output="articles1000.json"):
    data = []

    if os.path.exists(output):
        try:
            with open(output, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print("[ERROR] JSON file corrupted. Creating new one.")

    for url in urls:
        try:
            article_json = scrape_article(url)
            data.append(article_json)
        except Exception as e:
            print(f"[ERROR] Could not scrape {url}: {e}")

    with open(output, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"\n[✓] Saved {len(data)} total articles to {output}")

if __name__ == "__main__":

    
    with open("olympics_urls_clean3.txt", "r", encoding="utf-8") as f:
       urls = [line.strip('", \n') for line in f if line.strip()]

    #urls=["https://apnews.com/article/paris-olympics-gymnastics-medal-chiles-romania-a29e29e76f7e9e313e3d14179c3e5340"]

    scrape_multiple(urls)