#!/usr/bin/env python3
"""
cc_index_only.py

Harvest all YouTube channel homepage URLs that Common Crawl has fetched,
using the Index-Only method with automatic 429 back-off, and stream them
into a BigQuery table.
"""

import os
import sys
import json
import re
import time
import argparse
import requests
from tqdm import tqdm
from google.cloud import bigquery

PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*"
]

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot",   required=True)
    p.add_argument("--dataset",    required=True)
    p.add_argument("--table",      required=True)
    p.add_argument("--project",    default=None)
    p.add_argument("--max-pages",  type=int, default=10000)
    p.add_argument("--batch-size", type=int, default=500)
    return p.parse_args()

def normalize_url(raw_url: str) -> str:
    scheme_re = re.compile(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", re.IGNORECASE)
    path = scheme_re.sub("", raw_url)
    path = path.split("?",1)[0].split("#",1)[0].rstrip("/")
    parts = path.split("/",1)
    if len(parts)!=2: return ""
    return "https://www.youtube.com/" + parts[1]

def fetch_index_records(snapshot, pattern, page, max_retries=5):
    url = (f"https://index.commoncrawl.org/{snapshot}-index"
           f"?url={pattern}&output=json&page={page}")
    for attempt in range(max_retries):
        resp = requests.get(url, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.text.splitlines()
    raise RuntimeError(f"Rate limit at page {page}")

def save_batch_to_bq(client, table_ref, rows):
    errors = client.insert_rows_json(table_ref, [{"url":u} for u in rows])
    if errors:
        print("BigQuery insert errors:", errors, file=sys.stderr)

def main():
    args = parse_args()
    bq_client = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = bq_client.dataset(args.dataset).table(args.table)
    seen = set()
    total_new = 0

    for pattern in PATTERNS:
        print(f"\n=== Pattern: {pattern} ===")
        for page in tqdm(range(args.max_pages), desc="pages"):
            try:
                lines = fetch_index_records(args.snapshot, pattern, page)
            except Exception as e:
                print(f"Stopping {pattern} at page {page}: {e}", file=sys.stderr)
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
                    total_new += 1
                if len(batch) >= args.batch_size:
                    save_batch_to_bq(bq_client, table_ref, batch)
                    batch.clear()

            if batch:
                save_batch_to_bq(bq_client, table_ref, batch)

    print(f"\n✅ Done. Total unique channel URLs inserted: {total_new}")

if __name__=="__main__":
    main()
