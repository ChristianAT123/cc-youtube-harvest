# File: wayback_cdx_sourcer.py
#!/usr/bin/env python3

import argparse
import datetime
import requests
from urllib.parse import urlparse, unquote
from google.cloud import bigquery

def parse_args():
    parser = argparse.ArgumentParser(
        description="Harvest YouTube channel URLs from Wayback CDX API and insert into BigQuery"
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="Start date (YYYY-MM-DD) to resume harvesting"
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.datetime.utcnow(),
        help="End date (YYYY-MM-DD). Defaults to now"
    )
    parser.add_argument(
        "--bq-dataset", required=True, help="BigQuery dataset name"
    )
    parser.add_argument(
        "--bq-table", required=True, help="BigQuery table name"
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Number of rows to batch insert into BigQuery"
    )
    return parser.parse_args()

def generate_month_ranges(start_date, end_date):
    start = start_date.replace(day=1)
    while start <= end_date:
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1, day=1)
        else:
            next_month = start.replace(month=start.month + 1, day=1)
        yield start.strftime("%Y%m%d"), (next_month - datetime.timedelta(days=1)).strftime("%Y%m%d")
        start = next_month

def query_cdx(from_ts, to_ts):
    url = "http://web.archive.org/cdx/search/cdx"
    params = {
        "url": "www.youtube.com/*",
        "matchType": "host",
        "output": "json",
        "fl": "original",
        "from": from_ts,
        "to": to_ts,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    for row in data[1:]:
        yield row[0]

def normalize_url(raw_url):
    parsed = urlparse(raw_url)
    path = unquote(parsed.path)
    # convert /browse/...UC... → /channel/UC...
    if "/browse/" in path and "UC" in path:
        idx = path.find("UC")
        channel_id = path[idx:]
        return f"https://www.youtube.com/channel/{channel_id}"
    for prefix in ["/@", "/c/", "/channel/"]:
        if path.startswith(prefix):
            return f"https://www.youtube.com{path.rstrip('/')}"
    return None

def fetch_existing(bq_client, dataset, table):
    query = f"SELECT channel_url FROM `{dataset}.{table}`"
    rows = bq_client.query(query).result()
    return {row.channel_url for row in rows}

def insert_rows(bq_client, dataset, table, rows):
    table_ref = bq_client.dataset(dataset).table(table)
    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        print("Encountered errors while inserting rows:", errors)

def main():
    args = parse_args()
    start_date = args.start_date or datetime.datetime(2018, 1, 1)
    end_date = args.end_date
    bq_client = bigquery.Client()
    existing = fetch_existing(bq_client, args.bq_dataset, args.bq_table)
    batch = []
    for from_ts, to_ts in generate_month_ranges(start_date, end_date):
        print(f"Processing snapshot from {from_ts} to {to_ts}")
        for raw_url in query_cdx(from_ts, to_ts):
            norm = normalize_url(raw_url)
            if not norm or norm in existing:
                continue
            existing.add(norm)
            batch.append({"channel_url": norm})
            if len(batch) >= args.batch_size:
                insert_rows(bq_client, args.bq_dataset, args.bq_table, batch)
                batch.clear()
    if batch:
        insert_rows(bq_client, args.bq_dataset, args.bq_table, batch)

if __name__ == "__main__":
    main()
