#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
from requests.exceptions import HTTPError, ReadTimeout, RequestException
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from datetime import timedelta

# Only match real channel URL prefixes (including UC IDs)
PATTERNS = [
    ("prefix", "www.youtube.com/@"),
    ("prefix", "www.youtube.com/c/"),
    ("prefix", "www.youtube.com/channel/UC"),
    ("prefix", "www.youtube.com/user/"),
    ("prefix", "www.youtube.com/+"),
]

def parse_args():
    parser = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from Wayback CDX API into BigQuery"
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.datetime.utcnow(),
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument("--bq-dataset",   required=True, help="BigQuery dataset")
    parser.add_argument("--bq-table",     required=True, help="BigQuery table")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per insertion batch"
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=7,
        help="Number of days per CDX query window"
    )
    return parser.parse_args()

def generate_date_ranges(start, end, delta_days=7):
    """
    Yield (from_ts, to_ts) for consecutive windows of delta_days,
    formatted as YYYYMMDD strings.
    """
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=delta_days - 1), end)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)

def query_cdx(match_type, pattern, from_ts, to_ts, max_retries=3, timeout=60):
    url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":       pattern,
        "matchType": match_type,
        "output":    "json",
        "fl":        "original",
        "from":      from_ts,
        "to":        to_ts,
        "filter":    "statuscode:200",  # only 200-OK captures
        "collapse":  "urlkey",          # dedupe exact URLs
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            rows = [r[0] for r in data[1:]]
            print(f"  Retrieved {len(rows)} records for {pattern} ({from_ts}→{to_ts})")
            return rows
        except (HTTPError, ReadTimeout, RequestException) as e:
            print(f"  ⚠ Error {attempt}/{max_retries} for {pattern} ({from_ts}→{to_ts}): {e}")
            if attempt < max_retries:
                time.sleep(attempt * 5)
    print(f"  ↪ Skipping {pattern} for {from_ts}→{to_ts} after {max_retries} failures")
    return []

def normalize_url(raw):
    """
    - Reject bare prefixes (e.g. /channel/UC with no ID)
    - Turn /browse/...UC... into /channel/UCxxxx
    - Keep only valid prefix paths
    """
    p = urlparse(raw)
    path = unquote(p.path)
    # drop *exact* prefix-only hits
    bare = path.rstrip("/")
    if bare in ("/@", "/c", "/channel/UC", "/user", "/+"):
        return None

    # handle legacy /browse/...UCxxx
    if "/browse/" in path and "UC" in path:
        i = path.find("UC")
        return f"https://www.youtube.com/channel/{path[i:]}"
    # normal prefixes
    for prefix in ["/@", "/c/", "/channel/", "/user/", "/+/"]:
        if path.startswith(prefix):
            return f"https://www.youtube.com{path}".rstrip('/')
    return None

def fetch_existing(client, ds, tbl):
    sql = f"SELECT url FROM `{ds}.{tbl}`"
    return {row.url for row in client.query(sql).result()}

def insert_rows(client, ds, tbl, rows):
    table_ref = client.dataset(ds).table(tbl)
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print("  ❌ Insert errors:", errors)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018, 1, 1)
    end   = args.end_date
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    total_inserted = 0
    batch = []

    for frm, to in generate_date_ranges(start, end, delta_days=args.window_size):
        print(f"\nProcessing window {frm} → {to}")
        new_count = 0
        for match_type, pattern in PATTERNS:
            print(f"--- Pattern: {pattern} ({match_type}) ---")
            for raw in query_cdx(match_type, pattern, frm, to):
                norm = normalize_url(raw)
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                new_count += 1
                batch.append({
                    "url":         norm,
                    "source":      "wayback",
                    "ingested_at": datetime.datetime.utcnow().isoformat()
                })
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total_inserted += len(batch)
                    batch.clear()
        print(f"  New candidates this window: {new_count}")

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total_inserted += len(batch)

    print(f"\nTotal unique channels added: {total_inserted}")

if __name__ == "__main__":
    main()
