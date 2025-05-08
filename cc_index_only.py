#!/usr/bin/env python3
"""
cc_index_only.py

Harvest all YouTube channel homepage URLs that Common Crawl has fetched,
using the Index-Only method, and stream them into a BigQuery table.

Usage:
    python cc_index_only.py \
        --snapshot CC-MAIN-2024-10 \
        --dataset youtube_harvest \
        --table channels \
        [--project my-gcp-project] \
        [--max-pages 10000] \
        [--batch-size 500]
"""
import os
import sys
import json
import re
import argparse
import requests
from tqdm import tqdm
from google.cloud import bigquery

# ─── URL PATTERNS ─────────────────────────────────────────────────────────────
PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*"
]
# ───────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot",   required=True,
                   help="Common Crawl snapshot, e.g. CC-MAIN-2024-10")
    p.add_argument("--dataset",    required=True,
                   help="BigQuery dataset name")
    p.add_argument("--table",      required=True,
                   help="BigQuery table name")
    p.add_argument("--project",    default=None,
                   help="GCP project ID (defaults to env GCP_PROJECT or ADC)")
    p.add_argument("--max-pages",  type=int, default=10000,
                   help="Max pages per pattern")
    p.add_argument("--batch-size", type=int, default=500,
                   help="Rows per BigQuery insert")
    return p.parse_args()

def normalize_url(raw_url: str) -> str:
    """
    Canonicalize various scheme/subdomain variants into:
      https://www.youtube.com/...path...
    """
    # strip leading scheme (http(s):// or //), m., www.
    scheme_re = re.compile(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", re.IGNORECASE)
    path = scheme_re.sub("", raw_url)
    # remove query string or fragment
    path = path.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    # rebuild canonical form
    # after stripping, path == "youtube.com/<rest>" or "<domain>/<rest>"
    parts = path.split("/", 1)
    if len(parts) != 2:
        return ""
    return "https://www.youtube.com/" + parts[1]

def fetch_index_records(snapshot: str, pattern: str, page: int):
    """
    Fetch one page of Common Crawl Index API records for a given pattern.
    Returns a list of JSON strings (one per record).
    """
    url = (
        f"https://index.commoncrawl.org/{snapshot}-index"
        f"?url={pattern}&output=json&page={page}"
    )
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text.splitlines()

def save_batch_to_bq(client, table_ref, rows):
    """
    Insert a batch of { "url": ... } dicts into BigQuery.
    """
    errors = client.insert_rows_json(table_ref, [{"url": u} for u in rows])
    if errors:
        print("BigQuery insert errors:", errors, file=sys.stderr)

def main():
    args = parse_args()

    # BigQuery client
    bq_client = bigquery.Client(project=args.project) \
                if args.project else bigquery.Client()
    table_ref = bq_client.dataset(args.dataset).table(args.table)

    seen = set()
    total_new = 0

    for pattern in PATTERNS:
        print(f"\n=== Harvesting pattern: {pattern} ===")
        for page in tqdm(range(args.max_pages), desc="pages"):
            try:
                lines = fetch_index_records(args.snapshot, pattern, page)
            except Exception as e:
                print(f"Error fetching page {page} for {pattern}: {e}", file=sys.stderr)
                break
            if not lines:
                break

            batch = []
            for line in lines:
                rec = json.loads(line)
                raw = rec.get("url", "")
                clean = normalize_url(raw)
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

if __name__ == "__main__":
    main()
