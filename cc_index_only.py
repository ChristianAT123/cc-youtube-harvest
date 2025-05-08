#!/usr/bin/env python3
import argparse, json, re, sys, time
from urllib.parse import quote, unquote
import httpx
from tqdm import tqdm
from google.cloud import bigquery

PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*",
    "*.youtube.com/+*",
]
INDEX_URL = "https://index.commoncrawl.org/{snapshot}-index?url={enc}&output=json&page={page}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
COLLINFO_URL = "http://index.commoncrawl.org/collinfo.json"
DEFAULT_MAX_PAGES, DEFAULT_BATCH_SIZE = 10000, 500
ALLOWED_PREFIXES = ("/@", "/c/", "/channel/", "/user/", "/+")

httpx_client = httpx.Client(http2=True, headers={"User-Agent": USER_AGENT}, timeout=120)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--year", required=True)
    p.add_argument("--start-snapshot")
    p.add_argument("--pattern")
    p.add_argument("--dataset", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--project")
    p.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--collinfo-path")
    return p.parse_args()

def discover_snapshots(year, collinfo_path):
    if collinfo_path:
        data = json.load(open(collinfo_path))
    else:
        data = httpx_client.get(COLLINFO_URL).json()
    snaps = sorted(c["id"] for c in data if c["id"].startswith(f"CC-MAIN-{year}-"))
    if not snaps:
        sys.exit(1)
    return snaps

def normalize_url(raw):
    dec = unquote(raw.strip())
    scheme_re = re.compile(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", re.IGNORECASE)
    path = scheme_re.sub("", dec).split("?",1)[0].split("#",1)[0].rstrip("/")
    seg = path.split("/")
    if len(seg)==2 and seg[1]:
        return f"https://www.youtube.com/{seg[1]}"
    if seg[0]=="browse" and "-" in seg[-1]:
        c = seg[-1].split("-",1)[-1]
        if c.startswith("UC"):
            return f"https://www.youtube.com/channel/{c}"
    return ""

def fetch_index_records(snapshot, pattern, page):
    enc = quote(pattern, safe="/@+")
    url = INDEX_URL.format(snapshot=snapshot, enc=enc, page=page)
    for attempt in range(1,13):
        try:
            resp = httpx_client.get(url)
            if resp.status_code==400:
                return []
            if resp.status_code==429:
                time.sleep(min(2**attempt,60))
                continue
            resp.raise_for_status()
            return resp.text.splitlines()
        except httpx.HTTPError:
            if attempt<12:
                time.sleep(min(2**attempt,60))
            else:
                return []

def save_batch(client, table_ref, urls):
    client.insert_rows_json(table_ref, [{"url":u} for u in urls])

def main():
    args = parse_args()
    snaps = discover_snapshots(args.year, args.collinfo_path)
    if args.start_snapshot:
        idx = snaps.index(args.start_snapshot)
        snaps = snaps[idx:]
    patterns = [args.pattern] if args.pattern else PATTERNS

    client = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = client.dataset(args.dataset).table(args.table)
    seen = {r.url for r in client.query(f"SELECT url FROM `{client.project}.{args.dataset}.{args.table}`").result()}

    total_new = 0

    for snap in snaps:
        print(f"\n\n===== Snapshot: {snap} =====")
        for pattern in patterns:
            print(f"\n--- Pattern: {pattern} ---")
            for page in tqdm(range(args.max_pages), desc="pages", unit="page"):
                lines = fetch_index_records(snap, pattern, page)
                if not lines:
                    break
                batch = []
                for line in lines:
                    rec = json.loads(line)
                    clean = normalize_url(rec.get("url",""))
                    if clean and clean.replace("https://www.youtube.com","").startswith(ALLOWED_PREFIXES) and clean not in seen:
                        seen.add(clean)
                        batch.append(clean)
                        total_new += 1
                    if len(batch) >= args.batch_size:
                        save_batch(client, table_ref, batch)
                        batch.clear()
                if batch:
                    save_batch(client, table_ref, batch)

    print(f"\n🎉 Done. New unique URLs inserted: {total_new}")

if __name__=="__main__":
    main()
