#!/usr/bin/env python3

import argparse
import datetime
import time
import requests
import calendar
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from datetime import timedelta, timezone
from requests.exceptions import HTTPError, RequestException

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
    current = start_dt.replace(day=1)
    while current <= end_dt:
        y, m = current.year, current.month
        last = calendar.monthrange(y, m)[1]
        ms = current
        me = datetime.datetime(y, m, last, tzinfo=timezone.utc)
        if me > end_dt: me = end_dt
        yield ms, me
        # advance to next month
        if m == 12:
            current = current.replace(year=y+1, month=1, day=1)
        else:
            current = current.replace(month=m+1, day=1)

def week_ranges(start_dt, end_dt, days=7):
    cur = start_dt
    while cur <= end_dt:
        we = min(cur + timedelta(days=days-1), end_dt)
        yield cur.strftime("%Y%m%d"), we.strftime("%Y%m%d")
        cur = we + timedelta(days=1)

def stream_cdx(pattern, match_type, frm, to):
    """
    Try one resumeKey-stream; on HTTPError return None.
    """
    base = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":           pattern,
        "matchType":     match_type,
        "from":          frm,
        "to":            to,
        "filter":        "statuscode:200",
        "collapse":      "urlkey",
        "showResumeKey": "true",
        "output":        "json",
        "fl":            "original",
    }

    try:
        r = requests.get(base, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
    except (HTTPError, RequestException) as e:
        print(f"  ⚠ resumeKey stream failed for {pattern} ({frm}→{to}): {e}")
        return None

    all_urls = [rec[0] for rec in data[1:]]
    resume = data[-1][-1]
    while resume:
        params = {
            "url":       pattern,
            "matchType": match_type,
            "resumeKey": resume,
            "filter":    "statuscode:200",
            "collapse":  "urlkey",
            "output":    "json",
            "fl":        "original",
        }
        time.sleep(1)
        r = requests.get(base, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        urls = [rec[0] for rec in data[1:]]
        all_urls.extend(urls)
        resume = data[-1][-1]
    return all_urls

def normalize_url(raw):
    p = urlparse(raw)
    path = unquote(p.path)
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
    ref = client.dataset(ds).table(tbl)
    errs = client.insert_rows_json(ref, rows)
    if errs:
        print("  ❌ Insert errors:", errs)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def backfill():
    args   = parse_args()
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch  = []
    total  = 0

    start = args.start_date or datetime.datetime(2018,1,1,tzinfo=timezone.utc)
    end   = args.end_date

    for ms, me in month_boundaries(start, end):
        print(f"\n=== Month: {ms:%Y-%m} → {me:%Y-%m-%d} ===")
        for match_type, pattern in PATTERNS:
            print(f"--- Pattern: {pattern} ---")
            for frm, to in week_ranges(ms, me):
                print(f"→ Window {frm}→{to}")
                urls = stream_cdx(pattern, match_type, frm, to)
                # fallback to daily if week-level failed
                if urls is None:
                    for day_dt in (ms + timedelta(d) for d in range((me-ms).days+1)):
                        d = day_dt.strftime("%Y%m%d")
                        print(f"  ↳ Retry single-day {d}")
                        urls = stream_cdx(pattern, match_type, d, d) or []
                        for raw in urls:
                            norm = normalize_url(raw)
                            if not norm or norm in seen: continue
                            seen.add(norm)
                            batch.append({"url":norm,"source":"wayback","ingested_at":datetime.datetime.now(timezone.utc).isoformat()})
                        if len(batch)>=args.batch_size:
                            insert_rows(client,args.bq_dataset,args.bq_table,batch)
                            total += len(batch)
                            batch.clear()
                    continue

                print(f"   ■ {len(urls)} URLs")
                for raw in urls:
                    norm = normalize_url(raw)
                    if not norm or norm in seen: continue
                    seen.add(norm)
                    batch.append({"url":norm,"source":"wayback","ingested_at":datetime.datetime.now(timezone.utc).isoformat()})
                if len(batch)>=args.batch_size:
                    insert_rows(client,args.bq_dataset,args.bq_table,batch)
                    total += len(batch)
                    batch.clear()

    if batch:
        insert_rows(client,args.bq_dataset,args.bq_table,batch)
        total += len(batch)

    print(f"\n✅ Total unique channels added: {total}")

if __name__ == "__main__":
    backfill()
