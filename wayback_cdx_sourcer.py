#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
from requests.exceptions import HTTPError, ReadTimeout, RequestException
from urllib.parse import urlparse, unquote
from datetime import timedelta, timezone
from google.cloud import bigquery

# matchType + prefix combos we want
PATTERNS = [
    ("prefix", "www.youtube.com/@"),
    ("prefix", "www.youtube.com/c/"),
    ("prefix", "www.youtube.com/channel/UC"),
    ("prefix", "www.youtube.com/user/"),
    ("prefix", "www.youtube.com/+"),
]

def parse_args():
    p = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from Wayback CDX API into BigQuery"
    )
    p.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        default=None,
        help="YYYY-MM-DD, defaults to 2018-01-01"
    )
    p.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        default=datetime.datetime.now(timezone.utc),
        help="YYYY-MM-DD, defaults to now"
    )
    p.add_argument("--bq-dataset", required=True, help="BigQuery dataset")
    p.add_argument("--bq-table",   required=True, help="BigQuery table")
    p.add_argument(
        "--batch-size", type=int, default=500,
        help="Rows per insert batch"
    )
    p.add_argument(
        "--window-size", type=int, default=7,
        help="Initial days per CDX query (will auto-shrink on failures)"
    )
    return p.parse_args()

def generate_date_ranges(start_dt, end_dt, delta_days):
    cur = start_dt
    while cur <= end_dt:
        window_end = min(cur + timedelta(days=delta_days-1), end_dt)
        yield cur.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        cur = window_end + timedelta(days=1)

def query_cdx(match_type, pattern, frm, to, max_retries=3, timeout=60):
    url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":       pattern,
        "matchType": match_type,
        "output":    "json",
        "fl":        "original",
        "from":      frm,
        "to":        to,
        "filter":    "statuscode:200",
        "collapse":  "urlkey",
    }
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return [row[0] for row in data[1:]]
        except (HTTPError, ReadTimeout, RequestException) as e:
            print(f"  ⚠ Error {attempt}/{max_retries} for {pattern} ({frm}→{to}): {e}")
            if attempt < max_retries:
                time.sleep(attempt * 5)
    print(f"  ↪ Timeout for {pattern} ({frm}→{to})")
    return None

def normalize_url(raw):
    p = urlparse(raw)
    path = unquote(p.path)
    bare = path.rstrip("/")
    if bare in ("/@", "/c", "/channel/UC", "/user", "/+"):
        return None
    if "/browse/" in path and "UC" in path:
        i = path.find("UC")
        return f"https://www.youtube.com/channel/{path[i:]}"
    for pre in ("/@", "/c/", "/channel/", "/user/", "/+/"):
        if path.startswith(pre):
            return f"https://www.youtube.com{path}".rstrip("/")
    return None

def fetch_existing(client, ds, tbl):
    q = f"SELECT url FROM `{ds}.{tbl}`"
    return {row.url for row in client.query(q).result()}

def insert_rows(client, ds, tbl, rows):
    ref = client.dataset(ds).table(tbl)
    errors = client.insert_rows_json(ref, rows)
    if errors:
        print("  ❌ Insert errors:", errors)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def process_pattern(match_type, pattern, start_dt, end_dt, window_size,
                    client, ds, tbl, seen, batch, batch_size, total_count):
    def recurse(s_dt, e_dt, win):
        for frm, to in generate_date_ranges(s_dt, e_dt, win):
            rows = query_cdx(match_type, pattern, frm, to)
            if rows is None:
                if win > 1:
                    recurse(s_dt=datetime.datetime.strptime(frm, "%Y%m%d").replace(tzinfo=timezone.utc),
                            e_dt=datetime.datetime.strptime(to,   "%Y%m%d").replace(tzinfo=timezone.utc),
                            win=max(1, win//2))
                else:
                    print(f"  ↪ Skipping single-day {pattern} {frm}")
                continue

            for raw in rows:
                url = normalize_url(raw)
                if not url or url in seen:
                    continue
                seen.add(url)
                batch.append({
                    "url":         url,
                    "source":      "wayback",
                    "ingested_at": datetime.datetime.now(timezone.utc).isoformat()
                })
                if len(batch) >= batch_size:
                    insert_rows(client, ds, tbl, batch)
                    total_count[0] += len(batch)
                    batch.clear()

    recurse(start_dt, end_dt, window_size)

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018,1,1, tzinfo=timezone.utc)
    end   = args.end_date
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch  = []
    total_count = [0]

    for frm, to in generate_date_ranges(start, end, args.window_size):
        sd = datetime.datetime.strptime(frm, "%Y%m%d").replace(tzinfo=timezone.utc)
        ed = datetime.datetime.strptime(to,   "%Y%m%d").replace(tzinfo=timezone.utc)
        print(f"\n=== Window {frm} → {to} ===")
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ---")
            process_pattern(
                mt, pat, sd, ed, args.window_size,
                client, args.bq_dataset, args.bq_table,
                seen, batch, args.batch_size, total_count
            )

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total_count[0] += len(batch)

    print(f"\n✅ Total unique channels added: {total_count[0]}")

if __name__ == "__main__":
    main()
