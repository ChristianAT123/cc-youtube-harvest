#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
from requests.exceptions import HTTPError, ReadTimeout, RequestException
from urllib.parse import urlparse, unquote
from google.cloud import bigquery

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
    parser.add_argument("--bq-dataset", required=True, help="BigQuery dataset")
    parser.add_argument("--bq-table",   required=True, help="BigQuery table")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per insertion batch"
    )
    return parser.parse_args()

def generate_month_ranges(start, end):
    current = start.replace(day=1)
    while current <= end:
        nxt = (current.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        yield current.strftime("%Y%m%d"), (nxt - datetime.timedelta(days=1)).strftime("%Y%m%d")
        current = nxt

def query_cdx(match_type, pattern, from_ts, to_ts, max_retries=3, timeout=60):
    url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":       pattern,
        "matchType": match_type,
        "output":    "json",
        "fl":        "original",
        "from":      from_ts,
        "to":        to_ts,
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            rows = [r[0] for r in data[1:]]
            print(f"  Retrieved {len(rows)} records for {pattern}")
            return rows
        except (HTTPError, ReadTimeout, RequestException) as e:
            print(f"  ⚠ Error {attempt}/{max_retries} for {pattern}: {e}")
            if attempt < max_retries:
                time.sleep(attempt * 5)
    print(f"  ↪ Skipping {pattern} after {max_retries} failures")
    return []

def normalize_url(raw):
    p = urlparse(raw)
    path = unquote(p.path)
    if "/browse/" in path and "UC" in path:
        i = path.find("UC")
        return f"https://www.youtube.com/channel/{path[i:]}"
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

    for frm, to in generate_month_ranges(start, end):
        print(f"\nProcessing snapshot {frm} → {to}")
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
                    "url": norm,
                    "source": "wayback",
                    "ingested_at": datetime.datetime.utcnow().isoformat()
                })
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total_inserted += len(batch)
                    batch.clear()
        print(f"  New candidates this snapshot: {new_count}")

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total_inserted += len(batch)

    print(f"\nTotal unique channels added: {total_inserted}")

if __name__ == "__main__":
    main()
