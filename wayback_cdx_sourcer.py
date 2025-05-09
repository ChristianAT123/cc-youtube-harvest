# File: wayback_cdx_sourcer.py
#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
from requests.exceptions import HTTPError, ReadTimeout, RequestException
from urllib.parse import urlparse, unquote
from google.cloud import bigquery

PATTERNS = [
    "*.youtube.com/@*",
    "*.youtube.com/c/*",
    "*.youtube.com/channel/*",
    "*.youtube.com/user/*",
    "*.youtube.com/+*",
]

def parse_args():
    parser = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from Wayback CDX API into BigQuery"
    )
    parser.add_argument("--start-date", type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"), default=None)
    parser.add_argument("--end-date",   type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"), default=datetime.datetime.utcnow())
    parser.add_argument("--bq-dataset", required=True)
    parser.add_argument("--bq-table",   required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args()

def generate_month_ranges(start, end):
    current = start.replace(day=1)
    while current <= end:
        nxt = (current.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        yield current.strftime("%Y%m%d"), (nxt - datetime.timedelta(days=1)).strftime("%Y%m%d")
        current = nxt

def query_cdx_for_pattern(pattern, from_ts, to_ts, max_retries=3, timeout=60):
    url = "http://web.archive.org/cdx/search/cdx"
    params = {"url": pattern, "matchType": "glob", "output": "json", "fl": "original", "from": from_ts, "to": to_ts}
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            rows = data[1:]
            print(f"  Retrieved {len(rows)} rows for {pattern}")
            return [r[0] for r in rows]
        except (HTTPError, ReadTimeout, RequestException) as e:
            print(f"  ⚠ Error ({attempt}/{max_retries}) for {pattern} {from_ts}-{to_ts}: {e}")
            if attempt < max_retries:
                time.sleep(attempt * 5)
    print(f"  ↪ Skipping pattern {pattern} for {from_ts}-{to_ts}")
    return []

def normalize_url(raw):
    p = urlparse(raw)
    path = unquote(p.path)
    if "/browse/" in path and "UC" in path:
        idx = path.find("UC")
        return f"https://www.youtube.com/channel/{path[idx:]}"
    for prefix in ["/@", "/c/", "/channel/", "/user/", "/+"]:
        if path.startswith(prefix):
            return f"https://www.youtube.com{path.rstrip('/')}"
    return None

def fetch_existing(client, ds, tbl):
    sql = f"SELECT url FROM `{ds}.{tbl}`"
    return {r.url for r in client.query(sql).result()}

def insert_rows(client, ds, tbl, rows):
    errors = client.insert_rows_json(client.dataset(ds).table(tbl), rows)
    if errors:
        print("  ❌ Insert errors:", errors)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018, 1, 1)
    end   = args.end_date
    client = bigquery.Client()
    seen = fetch_existing(client, args.bq_dataset, args.bq_table)
    total_inserted = 0
    batch = []

    for frm, to in generate_month_ranges(start, end):
        print(f"\nProcessing snapshot {frm} → {to}")
        new_this_snapshot = 0
        for pattern in PATTERNS:
            print(f"--- Pattern: {pattern} ---")
            raw_urls = query_cdx_for_pattern(pattern, frm, to)
            for raw in raw_urls:
                norm = normalize_url(raw)
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                new_this_snapshot += 1
                batch.append({"url": norm, "source": "wayback", "ingested_at": datetime.datetime.utcnow().isoformat()})
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total_inserted += len(batch)
                    batch.clear()
        print(f"  New candidates in {frm}–{to}: {new_this_snapshot}")

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total_inserted += len(batch)

    print(f"\nTotal unique channels added: {total_inserted}")

if __name__ == "__main__":
    main()
