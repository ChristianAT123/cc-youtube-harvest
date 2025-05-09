# File: wayback_cdx_sourcer.py
#!/usr/bin/env python3

import argparse
import datetime
from datetime import timezone
import requests
from urllib.parse import urlparse, unquote
from google.cloud import bigquery

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
        default=datetime.datetime.now(timezone.utc),
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument("--bq-dataset", required=True, help="BigQuery dataset")
    parser.add_argument("--bq-table",   required=True, help="BigQuery table")
    parser.add_argument(
        "--batch-size", type=int, default=500,
        help="Rows per insertion batch"
    )
    return parser.parse_args()

def generate_month_ranges(start_date, end_date):
    current = start_date.replace(day=1)
    while current <= end_date:
        nxt = (current.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        yield current.strftime("%Y%m%d"), (nxt - datetime.timedelta(days=1)).strftime("%Y%m%d")
        current = nxt

def query_cdx(from_ts, to_ts):
    resp = requests.get(
        "http://web.archive.org/cdx/search/cdx",
        params={
            "url":       "www.youtube.com/*",
            "matchType": "host",
            "output":    "json",
            "fl":        "original",
            "from":      from_ts,
            "to":        to_ts,
        },
        timeout=60
    )
    resp.raise_for_status()
    for row in resp.json()[1:]:
        yield row[0]

def normalize_url(raw_url):
    p = urlparse(raw_url)
    path = unquote(p.path)
    if "/browse/" in path and "UC" in path:
        idx = path.find("UC")
        return f"https://www.youtube.com/channel/{path[idx:]}"
    for prefix in ["/@", "/c/", "/channel/", "/user/", "/+"]:
        if path.startswith(prefix):
            return f"https://www.youtube.com{path.rstrip('/')}"
    return None

def fetch_existing(client, dataset, table):
    sql = f"SELECT url FROM `{dataset}.{table}`"
    rows = client.query(sql).result()
    return {r.url for r in rows}

def insert_rows(client, dataset, table, rows):
    table_ref = client.dataset(dataset).table(table)
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print("Insert errors:", errors)

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018, 1, 1, tzinfo=timezone.utc)
    end   = args.end_date
    client = bigquery.Client()
    seen = fetch_existing(client, args.bq_dataset, args.bq_table)
    total_inserted = 0
    batch = []

    for frm, to in generate_month_ranges(start, end):
        print(f"Processing snapshot {frm} → {to}")
        for raw in query_cdx(frm, to):
            norm = normalize_url(raw)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            batch.append({
                "url":         norm,
                "source":      "wayback",
                "ingested_at": datetime.datetime.now(timezone.utc).isoformat()
            })
            if len(batch) >= args.batch_size:
                insert_rows(client, args.bq_dataset, args.bq_table, batch)
                total_inserted += len(batch)
                batch.clear()

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total_inserted += len(batch)

    print(f"Total unique channels added: {total_inserted}")

if __name__ == "__main__":
    main()
