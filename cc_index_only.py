#!/usr/bin/env python3
import argparse
import json
import re
import sys
import time
from urllib.parse import quote, unquote

import requests
from requests.exceptions import RequestException
from tqdm import tqdm
from google.cloud import bigquery

PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*",
    "*.youtube.com/+*",
]
USER_AGENT        = "Mozilla/5.0 (compatible; IndexFetcher/1.0)"
COLLINFO_URL      = "http://index.commoncrawl.org/collinfo.json"
INDEX_TEMPLATE    = "https://index.commoncrawl.org/{snapshot}-index?url={enc}&output=json&page={page}"
DEFAULT_MAX_PAGES = 10000
DEFAULT_BATCH     = 500
ALLOWED_PREFIXES  = ("/@", "/c/", "/channel/", "/user/", "/+")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--year",           required=True,          help="4‑digit year (e.g. 2024)")
    p.add_argument("--start-snapshot", default=None,          help="resume at this snapshot ID")
    p.add_argument("--pattern",        default=None,          help="override to a single glob pattern")
    p.add_argument("--dataset",        required=True,          help="BigQuery dataset")
    p.add_argument("--table",          required=True,          help="BigQuery table")
    p.add_argument("--project",        default=None,          help="GCP project (overrides ADC)")
    p.add_argument("--max-pages",      type=int, default=DEFAULT_MAX_PAGES,
                                                    help="max pages per pattern")
    p.add_argument("--batch-size",     type=int, default=DEFAULT_BATCH,
                                                    help="BigQuery insert batch size")
    p.add_argument("--collinfo-path",  default=None,          help="local collinfo.json path")
    return p.parse_args()

def discover_snapshots(year, path):
    if path:
        data = json.load(open(path))
    else:
        r = requests.get(COLLINFO_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
        r.raise_for_status()
        data = r.json()
    snaps = sorted(c["id"] for c in data if c["id"].startswith(f"CC-MAIN-{year}-"))
    if not snaps:
        print(f"❌ No snapshots for year {year}", file=sys.stderr)
        sys.exit(1)
    return snaps

def normalize_url(raw):
    dec = unquote(raw.strip())
    dec = re.sub(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", "", dec, flags=re.IGNORECASE)
    path = dec.split("?",1)[0].split("#",1)[0].rstrip("/")
    seg = path.split("/")
    if len(seg)==2 and seg[1]:
        return f"https://www.youtube.com/{seg[1]}"
    if seg[0]=="browse" and "-" in seg[-1]:
        c = seg[-1].split("-",1)[-1]
        if c.startswith("UC"):
            return f"https://www.youtube.com/channel/{c}"
    return ""

def fetch_page(snapshot, pattern, page):
    enc = quote(pattern, safe="*/@+")
    url = INDEX_TEMPLATE.format(snapshot=snapshot, enc=enc, page=page)
    backoff = 1
    for i in range(1,6):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
            if r.status_code==400:
                return []
            if r.status_code==429:
                time.sleep(backoff); backoff*=2; continue
            r.raise_for_status()
            return r.text.splitlines()
        except RequestException:
            if i<5:
                time.sleep(backoff); backoff*=2
            else:
                return []

def save_batch(client, table, urls):
    rows = [{"url":u} for u in urls]
    errs = client.insert_rows_json(table, rows)
    if errs:
        print("❌ BQ errors:", errs, file=sys.stderr)

def main():
    args = parse_args()
    snaps = discover_snapshots(args.year, args.collinfo_path)
    if args.start_snapshot:
        i = snaps.index(args.start_snapshot)
        snaps = snaps[i:]

    patterns = [args.pattern] if args.pattern else PATTERNS

    client    = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = client.dataset(args.dataset).table(args.table)
    full      = f"{client.project}.{args.dataset}.{args.table}"

    print(f"✅ Found {len(snaps)} snapshots for {args.year}")
    print("Loading existing URLs from BigQuery…")
    existing = client.query(f"SELECT url FROM `{full}`").result()
    seen = {r.url for r in existing}
    print(f"✅ Preloaded {len(seen)} existing URLs")

    total_new = 0

    for snap in snaps:
        print(f"\n===== Snapshot: {snap} =====")
        for pat in patterns:
            print(f"--- Pattern: {pat} ---")
            for pg in tqdm(range(args.max_pages), desc="pages", unit="page"):
                lines = fetch_page(snap, pat, pg)
                if not lines:
                    break

                batch = []
                for L in lines:
                    try:
                        rec = json.loads(L)
                    except:
                        continue
                    clean = normalize_url(rec.get("url",""))
                    suf   = clean.replace("https://www.youtube.com","")
                    if clean and suf.startswith(ALLOWED_PREFIXES) and clean not in seen:
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
