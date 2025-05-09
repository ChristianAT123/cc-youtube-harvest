#!/usr/bin/env python3

import argparse, datetime, time, requests, calendar
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
    p = argparse.ArgumentParser("Daily, paged CDX backfill")
    p.add_argument("--start-date",  type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc), default=None)
    p.add_argument("--end-date",    type=lambda s: datetime.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=timezone.utc), default=datetime.datetime.now(timezone.utc))
    p.add_argument("--bq-dataset",   required=True)
    p.add_argument("--bq-table",     required=True)
    p.add_argument("--batch-size",   type=int, default=500)
    p.add_argument("--page-size",    type=int, default=1000)
    return p.parse_args()

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def fetch_page(pattern, mt, frm, to, page, limit):
    url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":        pattern,
        "matchType":  mt,
        "from":       frm,
        "to":         to,
        "output":     "json",
        "fl":         "original",
        "filter":     "statuscode:200",
        "collapse":   "urlkey",
        "limit":      limit,
        "page":       page,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return [r[0] for r in data[1:]]

def normalize(raw):
    p = urlparse(raw).path
    path = unquote(p)
    if path.rstrip("/") in ("/@", "/c", "/channel/UC", "/user", "/+"):
        return None
    if "/browse/" in path and "UC" in path:
        idx = path.find("UC")
        return f"https://www.youtube.com/channel/{path[idx:]}"
    for pre in ("/@", "/c/", "/channel/", "/user/", "/+/"):
        if path.startswith(pre):
            return f"https://www.youtube.com{path}".rstrip("/")
    return None

def fetch_existing(client, ds, tbl):
    rows = client.query(f"SELECT url FROM `{ds}.{tbl}`").result()
    return {r.url for r in rows}

def insert_rows(client, ds, tbl, rows):
    ref = client.dataset(ds).table(tbl)
    errs = client.insert_rows_json(ref, rows)
    if errs: print("Insert errors:", errs)
    else:     print(f"Inserted {len(rows)} rows")

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018,1,1,tzinfo=timezone.utc)
    end   = args.end_date
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch  = []
    total  = 0

    for single in daterange(start, end):
        day = single.strftime("%Y%m%d")
        print(f"\n=== Date: {day} ===")
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ---")
            page = 0
            while True:
                try:
                    raws = fetch_page(pat, mt, day, day, page, args.page_size)
                except Exception as e:
                    print(f"  ⚠ page {page} failed: {e}")
                    break
                if not raws:
                    break
                print(f"  ▶ Day {day}, page {page}: {len(raws)} hits")
                for raw in raws:
                    url = normalize(raw)
                    if not url or url in seen: continue
                    seen.add(url)
                    batch.append({
                        "url":         url,
                        "source":      "wayback",
                        "ingested_at": datetime.datetime.now(timezone.utc).isoformat()
                    })
                if len(batch) >= args.batch_size:
                    insert_rows(client, args.bq_dataset, args.bq_table, batch)
                    total += len(batch)
                    batch.clear()
                if len(raws) < args.page_size:
                    break
                page += 1
                time.sleep(1)  # avoid hammering

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total += len(batch)

    print(f"\nDone — total new: {total}")

if __name__ == "__main__":
    main()
