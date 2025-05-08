#!/usr/bin/env python3
"""
cc_index_only.py

Harvest YouTube channel homepage URLs via the Common Crawl Index API
across every snapshot in a given year, streaming *only new* URLs into BigQuery.
Supports reading a pre‑downloaded collinfo.json to avoid remote fetch failures.
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

# ─── Configuration ────────────────────────────────────────────────────────────
PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*",
    "*.youtube.com/+*",
]
USER_AGENT         = "Mozilla/5.0 (compatible; IndexFetcher/1.0)"
DEFAULT_MAX_PAGES  = 10000
DEFAULT_BATCH_SIZE = 500
COLLINFO_URL       = "http://index.commoncrawl.org/collinfo.json"
# ───────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from all CC snapshots in a year"
    )
    p.add_argument("--year",          required=True, help="4-digit year to scan (e.g. 2024)")
    p.add_argument("--dataset",       required=True, help="BigQuery dataset")
    p.add_argument("--table",         required=True, help="BigQuery table")
    p.add_argument("--project",       default=None, help="GCP project ID (overrides ADC)")
    p.add_argument("--max-pages",     type=int, default=DEFAULT_MAX_PAGES,
                   help="Max pages per pattern")
    p.add_argument("--batch-size",    type=int, default=DEFAULT_BATCH_SIZE,
                   help="Rows per insert")
    p.add_argument("--collinfo-path", default=None,
                   help="Path to a pre-downloaded collinfo.json")
    return p.parse_args()

def discover_snapshots(year, collinfo_path=None):
    """
    Read snapshots from collinfo_path if provided, else fetch remotely.
    Return sorted list of CC-MAIN-<year>-XX IDs.
    """
    if collinfo_path:
        with open(collinfo_path, "r") as f:
            data = json.load(f)
    else:
        resp = requests.get(COLLINFO_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

    snaps = sorted(
        c["id"] for c in data
        if c["id"].startswith(f"CC-MAIN-{year}-")
    )
    if not snaps:
        print(f"❌ No snapshots found for year {year}", file=sys.stderr)
        sys.exit(1)
    return snaps

def normalize_url(raw: str) -> str:
    dec = unquote(raw.strip())
    scheme_re = re.compile(r"^(?:https?://|//)?(?:m\.)?(?:www\.)?", re.IGNORECASE)
    path = scheme_re.sub("", dec).split("?", 1)[0].split("#", 1)[0].rstrip("/")
    parts = path.split("/", 1)
    if len(parts) != 2 or not parts[1]:
        return ""
    return f"https://www.youtube.com/{parts[1]}"

def fetch_index_records(snapshot, pattern, page, retries=5):
    enc = quote(pattern, safe="*/@+")
    url = f"https://index.commoncrawl.org/{snapshot}-index?url={enc}&output=json&page={page}"
    headers = {"User-Agent": USER_AGENT}
    backoff = 1

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", backoff))
                time.sleep(wait)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.text.splitlines()
        except RequestException as e:
            if attempt < retries:
                print(f"❗ fetch retry {attempt} for {snapshot} {pattern} page {page}: {e}", file=sys.stderr)
                time.sleep(backoff)
                backoff *= 2
            else:
                raise RuntimeError(f"Failed fetch page={page} pattern={pattern} snapshot={snapshot}: {e}")

def save_batch(client, table_ref, urls):
    rows = [{"url": u} for u in urls]
    errs = client.insert_rows_json(table_ref, rows)
    if errs:
        print("❌ BQ insert errors:", errs, file=sys.stderr)

def main():
    args = parse_args()

    snaps = discover_snapshots(args.year, args.collinfo_path)
    print(f"✅ Found {len(snaps)} snapshots for year {args.year}")

    client = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = client.dataset(args.dataset).table(args.table)
    full_table = f"{client.project}.{args.dataset}.{args.table}"

    print("Loading existing URLs from BigQuery…")
    existing = client.query(f"SELECT url FROM `{full_table}`").result()
    seen = {row.url for row in existing}
    print(f"✅ Preloaded {len(seen)} existing URLs")

    total_new = 0

    for snap in snaps:
        print(f"\n\n===== Snapshot: {snap} =====")
        for pattern in PATTERNS:
            print(f"\n--- Pattern: {pattern} ---")
            for page in tqdm(range(args.max_pages), desc="pages", unit="page"):
                try:
                    lines = fetch_index_records(snap, pattern, page)
                except Exception as e:
                    print(f"⛔ Stopping {pattern}@{snap} page {page}: {e}", file=sys.stderr)
                    break
                if not lines:
                    break

                batch = []
                for line in lines:
                    rec = json.loads(line)
                    clean = normalize_url(rec.get("url", ""))
                    if clean and clean not in seen:
                        seen.add(clean)
                        batch.append(clean)
                        total_new += 1
                    if len(batch) >= args.batch_size:
                        save_batch(client, table_ref, batch)
                        batch.clear()

                if batch:
                    save_batch(client, table_ref, batch)

    print(f"\n🎉 Done. New unique URLs inserted: {total_new}")

if __name__ == "__main__":
    main()
