#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
import calendar
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from datetime import timedelta, timezone

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
    return p.parse_args()

def month_boundaries(start_dt, end_dt):
    """Yield (month_start, month_end) for each calendar month."""
    current = start_dt.replace(day=1)
    while current <= end_dt:
        year, month = current.year, current.month
        last = calendar.monthrange(year, month)[1]
        ms = current
        me = datetime.datetime(year, month, last, tzinfo=timezone.utc)
        if me > end_dt:
            me = end_dt
        yield ms, me
        # advance to first of next month
        if month == 12:
            current = current.replace(year=year+1, month=1, day=1)
        else:
            current = current.replace(month=month+1, day=1)

def query_cdx_paginated(match_type, pattern, frm, to):
    """
    Use CDX pagination so we never time out on large windows.
    """
    base = "https://web.archive.org/cdx/search/cdx"
    common = {
        "url":       pattern,
        "matchType": match_type,
        "filter":    "statuscode:200",
        "collapse":  "urlkey",
    }

    # 1) ask how many pages
    resp = requests.get(base, params={**common, "showNumPages": "true"})
    resp.raise_for_status()
    pages = int(resp.text.strip())

    all_rows = []
    for page in range(pages):
        params = {
            **common,
            "page":   page,
            "from":   frm,
            "to":     to,
            "output": "json",
            "fl":     "original",
        }
        r = requests.get(base, params=params)
        r.raise_for_status()
        data = r.json()
        rows = [r0[0] for r0 in data[1:]]
        print(f"  ▶ Page {page+1}/{pages}: {len(rows)} records for {pattern} ({frm}→{to})")
        all_rows.extend(rows)
        # be polite
        time.sleep(1)

    return all_rows

def normalize_url(raw):
    p = urlparse(raw)
    path = unquote(p.path)
    # drop bare prefixes
    if path.rstrip("/") in ("/@", "/c", "/channel/UC", "/user", "/+"):
        return None
    if "/browse/" in path and "UC" in path:
        i = path.find("UC")
        return f"https://www.youtube.com/channel/{path[i:]}"
    for pre in ("/@", "/c/", "/channel/", "/user/", "/+/"):
        if path.startswith(pre):
            return f"https://www.youtube.com{path}".rstrip("/")
    return None

def fetch_existing(client, ds, tbl):
    sql = f"SELECT url FROM `{ds}.{tbl}`"
    return {row.url for row in client.query(sql).result()}

def insert_rows(client, ds, tbl, rows):
    table = client.dataset(ds).table(tbl)
    errs = client.insert_rows_json(table, rows)
    if errs:
        print("  ❌ Insert errors:", errs)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018,1,1, tzinfo=timezone.utc)
    end   = args.end_date

    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch  = []
    total  = 0

    for ms, me in month_boundaries(start, end):
        frm = ms.strftime("%Y%m%d")
        to  = me.strftime("%Y%m%d")
        print(f"\n=== Month: {ms:%Y-%m} → {me:%Y-%m-%d} ===")
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ({mt}) ---")
            raws = query_cdx_paginated(mt, pat, frm, to)
            for raw in raws:
                url = normalize_url(raw)
                if not url or url in seen:
                    continue
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

    # flush remainder
    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total += len(batch)

    print(f"\n✅ Total unique channels added: {total}")

if __name__ == "__main__":
    main()
