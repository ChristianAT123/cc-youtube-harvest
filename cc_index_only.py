#!/usr/bin/env python3
"""
cc_index_only.py

Harvest all YouTube channel homepage URLs via the Common Crawl Index API
(“Index-Only” method) across every snapshot in a given year, and stream
unique, cleaned URLs into BigQuery.
"""

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

# ─── Config ───────────────────────────────────────────────────────────────────
YEAR             = None   # filled from args.year
PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*",
    "*.youtube.com/+*"
]
USER_AGENT       = "Mozilla/5.0 (compatible; IndexFetcher/1.0)"
DEFAULT_MAX_PAGES  = 10000
DEFAULT_BATCH_SIZE = 500
COLLINFO_URL      = "https://index.commoncrawl.org/collinfo.json"
# ───────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from all Common Crawl snapshots in a year"
    )
    p.add_argument("--year",     required=True,
                   help="4-digit year to scan (e.g. 2024)")
    p.add_argument("--dataset",  required=True, help="BigQuery dataset")
    p.add_argument("--table",    required=True, help="BigQuery table")
    p.add_argument("--project",  default=None,
                   help="GCP project ID (overrides ADC)")
    p.add_argument("--max-pages",  type=int, default=DEFAULT_MAX_PAGES,
                   help="Max pages per pattern")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                   help="Rows per insert")
    return p.parse_args()

def discover_snapshots(year):
    resp = requests.get(COLLINFO_URL, timeout=30)
    resp.raise_for_status()
    ids = [c["id"] for c in resp.json()
           if c["id"].startswith(f"CC-MAIN-{year}-")]
    return sorted(ids)

def normalize_url(raw_url: str) -> str:
    decoded = unquote(raw_url.strip())
    scheme_re = re.compile(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", re.IGNORECASE)
    path = scheme_re.sub("", decoded).split("?",1)[0].split("#",1)[0].rstrip("/")
    parts = path.split("/",1)
    if len(parts)!=2 or not parts[1]:
        return ""
    return "https://www.youtube.com/" + parts[1]

def fetch_index_records(snapshot, pattern, page, max_retries=5):
    enc = quote(pattern, safe="*/@+")
    url = f"https://index.commoncrawl.org/{snapshot}-index?url={enc}&output=json&page={page}"
    headers = {"User-Agent": USER_AGENT}
    backoff = 1
    for _ in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)
        except RequestException:
            time.sleep(backoff); backoff*=2; continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", backoff))
            time.sleep(wait); backoff*=2; continue

        r.raise_for_status()
        return r.text.splitlines()
    raise RuntimeError(f"Failed page={page} pattern={pattern} snapshot={snapshot}")

def save_batch_to_bq(client, table_ref, urls):
    rows = [{"url":u} for u in urls]
    errs = client.insert_rows_json(table_ref, rows)
    if errs:
        print("BQ insert errors:", errs, file=sys.stderr)

def main():
    args = parse_args()
    # discover
    snaps = discover_snapshots(args.year)
    if not snaps:
        print(f"No snapshots found for year {args.year}", file=sys.stderr)
        sys.exit(1)

    # BigQuery client
    bq = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = bq.dataset(args.dataset).table(args.table)

    seen = set()
    total = 0

    for snap in snaps:
        print(f"\n\n***** Snapshot: {snap} *****")
        for pattern in PATTERNS:
            print(f"\n=== Pattern: {pattern} ===")
            for page in tqdm(range(args.max_pages), desc="pages"):
                try:
                    lines = fetch_index_records(snap, pattern, page)
                except Exception as e:
                    print(f"Stopping {pattern}@{snap} page {page}: {e}", file=sys.stderr)
                    break
                if not lines:
                    break

                batch = []
                for line in lines:
                    rec = json.loads(line)
                    clean = normalize_url(rec.get("url",""))
                    if clean and clean not in seen:
                        seen.add(clean)
                        batch.append(clean)
                        total += 1
                    if len(batch) >= args.batch_size:
                        save_batch_to_bq(bq, table_ref, batch)
                        batch.clear()

                if batch:
                    save_batch_to_bq(bq, table_ref, batch)

    print(f"\n✅ Done. Total unique channel URLs inserted: {total}")

if __name__=="__main__":
    main()
