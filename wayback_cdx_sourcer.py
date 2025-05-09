#!/usr/bin/env python3
import argparse, datetime, time, requests
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from datetime import timedelta, timezone
from requests.exceptions import HTTPError, RequestException, ConnectionError

PATTERNS = [
    ("prefix", "www.youtube.com/@"),
    ("prefix", "www.youtube.com/c/"),
    ("prefix", "www.youtube.com/channel/UC"),
    ("prefix", "www.youtube.com/user/"),
    ("prefix", "www.youtube.com/+"),
]

def parse_args():
    p = argparse.ArgumentParser("Daily, paged CDX backfill")
    p.add_argument("--start-date",  type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc), default=None)
    p.add_argument("--end-date",    type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc), default=datetime.datetime.now(timezone.utc))
    p.add_argument("--bq-dataset",   required=True)
    p.add_argument("--bq-table",     required=True)
    p.add_argument("--batch-size",   type=int, default=500)
    p.add_argument("--page-size",    type=int, default=100)
    return p.parse_args()

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def fetch_page(pattern, mt, day, page, limit):
    url = "https://web.archive.org/cdx/search/cdx"
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
    for attempt in range(1, 5):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return [row[0] for row in r.json()[1:]]
        except ConnectionError:
            time.sleep(attempt * 2)
        except HTTPError as e:
            if 500 <= e.response.status_code < 600:
                time.sleep(attempt * 2)
            else:
                break
        except RequestException:
            break
    return []

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
    return {r.url for r in client.query(f"SELECT url FROM `{ds}.{tbl}`").result()}

def insert_rows(client, ds, tbl, rows):
    errs = client.insert_rows_json(client.dataset(ds).table(tbl), rows)
    if not errs:
        print(f"Inserted {len(rows)} rows")

def main():
    args  = parse_args()
    start = args.start_date or datetime.datetime(2018,1,1,tzinfo=timezone.utc)
    end   = args.end_date
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch, total = [], 0

    for single in daterange(start, end):
        day = single.strftime("%Y%m%d")
        print(f"\n=== Date: {day} ===")
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ---")
            page = 0
            while True:
                raws = fetch_page(pat, mt, day, page, args.page_size)
                if not raws:
                    break
                print(f"  ▶ page {page}: {len(raws)} hits")
                for raw in raws:
                    url = normalize(raw)
                    if url and url not in seen:
                        seen.add(url)
                        batch.append({"url":url,"source":"wayback","ingested_at":datetime.datetime.now(timezone.utc).isoformat()})
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total += len(batch)
                    batch.clear()
                if len(raws) < args.page_size:
                    break
                page += 1
                time.sleep(0.5)

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total += len(batch)

    print(f"\n✅ Total new: {total}")

if __name__ == "__main__":
    main()
