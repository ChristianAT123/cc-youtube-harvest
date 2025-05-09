#!/usr/bin/env python3

import argparse, datetime, time, requests
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from datetime import timedelta, timezone

PATTERNS = [
    ("prefix", "www.youtube.com/@"),
    ("prefix", "www.youtube.com/c/"),
    ("prefix", "www.youtube.com/channel/UC"),
    ("prefix", "www.youtube.com/user/"),
    ("prefix", "www.youtube.com/+"),
]

def parse_args():
    p = argparse.ArgumentParser("Daily, 100‑row paging CDX backfill")
    p.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc),
        default=None,
        help="YYYY-MM-DD, defaults to 2018-01-01"
    )
    p.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc),
        default=datetime.datetime.now(timezone.utc),
        help="YYYY-MM-DD, defaults to now"
    )
    p.add_argument("--bq-dataset", required=True, help="BigQuery dataset")
    p.add_argument("--bq-table",   required=True, help="BigQuery table")
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per BigQuery insert batch"
    )
    p.add_argument(
        "--page-size",
        type=int,
        default=50000,
        help="Max rows per CDX request"
    )
    return p.parse_args()

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def fetch_page(pattern, mt, day, page, limit):
    params = {
        "url":       pattern,
        "matchType": mt,
        "from":      day,
        "to":        day,
        "output":    "json",
        "fl":        "original",
        "filter":    "statuscode:200",
        "collapse":  "urlkey",
        "limit":     limit,
        "page":      page,
    }
    r = requests.get("https://web.archive.org/cdx/search/cdx", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return [row[0] for row in data[1:]]

def normalize(raw):
    path = unquote(urlparse(raw).path)
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
    return {r.url for r in client.query(q).result()}

def insert_rows(client, ds, tbl, rows):
    errs = client.insert_rows_json(client.dataset(ds).table(tbl), rows)
    if errs:
        print("Insert errors:", errs)
    else:
        print(f"Inserted {len(rows)} rows")

def main():
    args    = parse_args()
    start   = args.start_date or datetime.datetime(2018,1,1,tzinfo=timezone.utc)
    end     = args.end_date
    client  = bigquery.Client()
    seen    = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch   = []
    total   = 0

    for single in daterange(start, end):
        day = single.strftime("%Y%m%d")
        print(f"\n=== Date: {day} ===")
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ---")
            page = 0
            while True:
                try:
                    raws = fetch_page(pat, mt, day, page, args.page_size)
                except Exception as e:
                    print(f"  ⚠ page {page} failed: {e}")
                    break
                if not raws:
                    break
                print(f"  ▶ page {page}: {len(raws)} hits")
                for raw in raws:
                    url = normalize(raw)
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    batch.append({
                        "url":         url,
                        "source":      "wayback",
                        "ingested_at": datetime.datetime.now(timezone.utc).isoformat(),
                    })
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total += len(batch)
                    batch.clear()
                if len(raws) < args.page_size:
                    break
                page += 1
                time.sleep(0.5)  # gentle pacing

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total += len(batch)

    print(f"\n✅ Done — total new channels: {total}")

if __name__ == "__main__":
    main()
