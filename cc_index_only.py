#!/usr/bin/env python3
"""
cc_index_only.py

Harvest YouTube channel homepage URLs via the Common Crawl Index API
across every snapshot in a given year, streaming *only new* channel URLs into BigQuery.
Supports:
  - reading a pre‑downloaded collinfo.json to avoid remote fetch failures
  - percent‑encoding wildcard patterns for reliability
  - HTTP/2 requests via httpx with backoff & retry
  - a --start-snapshot flag to resume mid‑year
  - normalization of “/browse/...-UC...” URLs into /channel/UC...
  - deduplication against an existing BigQuery table
"""

import argparse
import json
import re
import sys
import time
from urllib.parse import quote, unquote

import httpx
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
INDEX_URL        = "https://index.commoncrawl.org/{snapshot}-index?url={enc}&output=json&page={page}"
COLLINFO_URL     = "http://index.commoncrawl.org/collinfo.json"
USER_AGENT       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
                   "AppleWebKit/537.36 (KHTML, like Gecko) " \
                   "Chrome/115.0.0.0 Safari/537.36"
DEFAULT_MAX_PAGES = 10000
DEFAULT_BATCH_SIZE = 500
ALLOWED_PREFIXES = ("/@", "/c/", "/channel/", "/user/", "/+")

# httpx client with HTTP/2 support and 120s timeout
httpx_client = httpx.Client(
    http2=True,
    headers={"User-Agent": USER_AGENT},
    timeout=120
)
# ───────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from Common Crawl indices"
    )
    p.add_argument("--year",            required=True,
                   help="4‑digit year to scan (e.g. 2024)")
    p.add_argument("--start-snapshot",  default=None,
                   help="ID of CC-MAIN snapshot to resume at")
    p.add_argument("--pattern",        default=None,
                   help="(Optional) single glob pattern override")
    p.add_argument("--dataset",         required=True,
                   help="BigQuery dataset to write into")
    p.add_argument("--table",           required=True,
                   help="BigQuery table to write into")
    p.add_argument("--project",         default=None,
                   help="GCP project ID (overrides ADC)")
    p.add_argument("--max-pages",       type=int, default=DEFAULT_MAX_PAGES,
                   help="Max pages per pattern")
    p.add_argument("--batch-size",      type=int, default=DEFAULT_BATCH_SIZE,
                   help="Rows per BigQuery insert")
    p.add_argument("--collinfo-path",   default=None,
                   help="Path to pre‑downloaded collinfo.json")
    return p.parse_args()


def discover_snapshots(year, collinfo_path=None):
    if collinfo_path:
        with open(collinfo_path, "r") as f:
            data = json.load(f)
    else:
        resp = httpx_client.get(COLLINFO_URL)
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
    seg = path.split("/")

    if len(seg) == 2 and seg[1]:
        return f"https://www.youtube.com/{seg[1]}"

    if seg[0] == "browse" and "-" in seg[-1]:
        cand = seg[-1].split("-", 1)[-1]
        if cand.startswith("UC"):
            return f"https://www.youtube.com/channel/{cand}"

    return ""


def fetch_index_records(snapshot: str, pattern: str, page: int):
    enc = quote(pattern, safe="/@+")
    url = INDEX_URL.format(snapshot=snapshot, enc=enc, page=page)

    for attempt in range(1, 12):
        try:
            resp = httpx_client.get(url)
            if resp.status_code == 400:
                return []
            if resp.status_code in (429, 503, 504):
                time.sleep(min(2 ** attempt, 60))
                continue
            resp.raise_for_status()
            return resp.text.splitlines()
        except httpx.HTTPError:
            if attempt < 11:
                time.sleep(min(2 ** attempt, 60))
                continue
            return []


def save_batch(client, table_ref, urls):
    rows = [{"url": u} for u in urls]
    errs = client.insert_rows_json(table_ref, rows)
    if errs:
        print("❌ BQ insert errors:", errs, file=sys.stderr)


def main():
    args = parse_args()

    snaps = discover_snapshots(args.year, args.collinfo_path)
    if args.start_snapshot:
        i = snaps.index(args.start_snapshot)
        snaps = snaps[i:]

    patterns = [args.pattern] if args.pattern else PATTERNS

    client = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    table_ref = client.dataset(args.dataset).table(args.table)
    full_table = f"{client.project}.{args.dataset}.{args.table}"

    print(f"✅ Found {len(snaps)} snapshots for {args.year}")
    print("Loading existing URLs from BigQuery…")
    existing = client.query(f"SELECT url FROM `{full_table}`").result()
    seen = {row.url for row in existing}
    print(f"✅ Preloaded {len(seen)} existing URLs")

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
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    clean = normalize_url(rec.get("url", ""))
                    suffix = clean.replace("https://www.youtube.com", "")
                    if clean and suffix.startswith(ALLOWED_PREFIXES) and clean not in seen:
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
