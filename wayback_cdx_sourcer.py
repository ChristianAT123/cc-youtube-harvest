#!/usr/bin/env python3
import argparse
import datetime
import time
import requests
import calendar
from requests.exceptions import HTTPError, ReadTimeout, RequestException
from urllib.parse import urlparse, unquote
from datetime import timedelta, timezone
from google.cloud import bigquery

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
        default=None, help="YYYY-MM-DD, defaults to 2018-01-01"
    )
    p.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        default=datetime.datetime.now(timezone.utc),
        help="YYYY-MM-DD, defaults to now"
    )
    p.add_argument("--bq-dataset", required=True)
    p.add_argument("--bq-table",   required=True)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--window-size", type=int, default=7)
    return p.parse_args()

def month_boundaries(start_dt, end_dt):
    """Yield (month_start_dt, month_end_dt) for each calendar month."""
    current = start_dt.replace(day=1)
    while current <= end_dt:
        year, month = current.year, current.month
        last_day = calendar.monthrange(year, month)[1]
        ms = current
        me = datetime.datetime(year, month, last_day, tzinfo=timezone.utc)
        if me > end_dt:
            me = end_dt
        yield ms, me
        # advance to 1st of next month
        if month == 12:
            current = current.replace(year=year+1, month=1, day=1)
        else:
            current = current.replace(month=month+1, day=1)

def date_windows(start_dt, end_dt, days):
    cur = start_dt
    while cur <= end_dt:
        we = min(cur + timedelta(days=days-1), end_dt)
        yield cur.strftime("%Y%m%d"), we.strftime("%Y%m%d")
        cur = we + timedelta(days=1)

def query_cdx(mt, pattern, frm, to, retries=3, timeout=60):
    url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url":      pattern,
        "matchType":mt,
        "output":   "json",
        "fl":       "original",
        "from":     frm,
        "to":       to,
        "filter":   "statuscode:200",
        "collapse": "urlkey",
    }
    for i in range(1, retries+1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return [row[0] for row in data[1:]]
        except (HTTPError, ReadTimeout, RequestException) as e:
            print(f"  ⚠ {i}/{retries} {pattern} ({frm}→{to}): {e}")
            if i < retries:
                time.sleep(i*5)
    print(f"  ↪ give up {pattern} ({frm}→{to})")
    return None

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
    qry = f"SELECT url FROM `{ds}.{tbl}`"
    return {row.url for row in client.query(qry).result()}

def insert_rows(client, ds, tbl, rows):
    ref = client.dataset(ds).table(tbl)
    errs = client.insert_rows_json(ref, rows)
    if errs:
        print("  ❌ Insert errors:", errs)
    else:
        print(f"  ▶ Inserted {len(rows)} rows")

def process_month(mt, pat, ms, me, args, client, seen, batch, total):
    """Slice [ms,me] into windows; auto-shrink on timeout; batch rows."""
    def recurse(sdt, edt, win):
        for frm, to in date_windows(sdt, edt, win):
            hits = query_cdx(mt, pat, frm, to)
            if hits is None:
                if win > 1:
                    recurse(sdt, edt, max(1, win//2))
                else:
                    print(f"    ↪ skip day {frm}")
                continue
            for raw in hits:
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
                    total[0] += len(batch)
                    batch.clear()
    print(f"\n=== Month: {ms.strftime('%Y-%m')} → {me.strftime('%Y-%m-%d')} ===")
    recurse(ms, me, args.window_size)

def main():
    args = parse_args()
    start = args.start_date or datetime.datetime(2018,1,1, tzinfo=timezone.utc)
    end   = args.end_date
    client = bigquery.Client()
    seen   = fetch_existing(client, args.bq_dataset, args.bq_table)
    batch  = []
    total  = [0]

    for ms, me in month_boundaries(start, end):
        for mt, pat in PATTERNS:
            print(f"--- Pattern: {pat} ---")
            process_month(mt, pat, ms, me, args, client, seen, batch, total)

    if batch:
        insert_rows(client, args.bq_dataset, args.bq_table, batch)
        total[0] += len(batch)

    print(f"\n✅ Total unique channels added: {total[0]}")

if __name__ == "__main__":
    main()
