"""
LWEA History Gap Fill
=====================
Version: 1.0.0

Fills weekly points that are MISSING from listing-stats/history.json, using real
Google Analytics and Search Console numbers for those weeks. Run it from the
"Fill History Gaps" GitHub Actions workflow (fill-history-gaps.yml).

Why it exists: after the 2026-10-05 run (see listing_stats_sync.py 1.5.7) a number
of listings restarted their weekly history in late September, and listings added
to the site after the one-time backfill only have points from their first sync.
Their charts are missing weeks that really happened, sometimes with real visits.

What it does, and what it never does
  * Only ADDS weeks that are not in a listing's series. It never changes, moves or
    deletes a point that already exists.
  * A week whose Google pull fails is SKIPPED, never saved as zero, so a failed
    call cannot turn into fake "no traffic" data. The report lists every skip.
  * Weeks before a listing was posted are left alone (the dashboard widget draws
    those as flat zeros). To catch a wrong posted date, it also looks back
    BUFFER_WEEKS before the posted week and reports any real traffic it finds there.
  * results.json is never touched.
  * Aborts without publishing if the merged file has fewer listings or fewer
    points than the file it started from.

Environment (same names as the other scripts):
  BD_API_KEY, GA4_PROPERTY_ID, GOOGLE_CREDENTIALS_PATH, GITHUB_TOKEN, GITHUB_REPO
  (mindthegaptutoring/lwea-search). Optional:
  ONLY_IDS   comma list such as "listing_138,listing_173" to limit the run
  DRY_RUN    "true" = pull and write the report, but publish nothing
  START_WEEK first Monday to fill from (default: oldest first point in history.json)
  BUFFER_WEEKS weeks to look back before the posted week (default 1)

Output: history_gap_fill_report.csv (one row per week added or skipped).
"""
import csv
import os
import sys
import time
from datetime import date, timedelta

import listing_stats_sync as S
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from googleapiclient.discovery import build as gbuild

REPORT_PATH = "history_gap_fill_report.csv"
BUFFER_WEEKS = int(os.environ.get("BUFFER_WEEKS", "1") or 1)
DRY_RUN = (os.environ.get("DRY_RUN", "false").strip().lower() == "true")
ONLY_IDS = {x.strip() for x in os.environ.get("ONLY_IDS", "").split(",") if x.strip()}
PAUSE_SECONDS = 0.25   # small gap between Google calls


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def parse_day(text: str):
    return date.fromisoformat(str(text)[:10])


def weeks_to_fill(row, series, start_week: date, last_complete: date):
    """Mondays that have no point yet, from max(start, posted week - buffer) to the last complete week."""
    posted = row.get("posted_date")
    floor = start_week
    if posted:
        floor = max(start_week, monday(parse_day(posted)) - timedelta(weeks=BUFFER_WEEKS))
    have = {str(p.get("date")) for p in series}
    out = []
    d = floor
    while d <= last_complete:
        if d.isoformat() not in have:
            out.append(d)
        d += timedelta(days=7)
    return out


def pull_week(gsc_service, ga4_client, urls, ws: date):
    """Real numbers for one full Monday to Sunday week. Raises on any failure."""
    we = ws + timedelta(days=6)
    gsc = S.fetch_period(
        lambda u, **kw: S.get_gsc_stats(gsc_service, u, include_queries=False, **kw),
        urls, start_override=ws, end_override=we)
    time.sleep(PAUSE_SECONDS)
    ga4 = S.fetch_period(
        lambda u, **kw: S.get_ga4_stats(ga4_client, u, **kw),
        urls, start_override=ws, end_override=we)
    time.sleep(PAUSE_SECONDS)
    return {
        "date": ws.isoformat(),
        "impressions": gsc.get("impressions", 0),
        "clicks": gsc.get("clicks", 0),
        "ctr": gsc.get("ctr", 0.0),
        "position": gsc.get("position", 0.0),
        "sessions": ga4.get("sessions", 0),
        "avg_engagement_seconds": ga4.get("avg_engagement_seconds", 0.0),
        "contact_clicks": ga4.get("contact_clicks"),
        "connect_pageviews": None,
        "triage_flag": None,
    }


def main():
    today = date.today()
    last_complete = monday(today) - timedelta(days=7)

    results, _ = S.fetch_existing_json(S.GITHUB_FILE_PATH)
    rows = (results or {}).get("listings") or []
    history_file, _ = S.fetch_existing_json(S.GITHUB_HISTORY_FILE_PATH)
    history = (history_file or {}).get("history") or {}
    if not rows or not history:
        raise SystemExit("results.json or history.json came back empty; nothing was changed.")

    firsts = [str(p[0]["date"]) for p in history.values() if p]
    start_week = parse_day(os.environ.get("START_WEEK") or min(firsts))
    start_week = monday(start_week)
    print(f"Filling gaps from {start_week} to {last_complete}. Dry run: {DRY_RUN}")

    creds = S.get_google_credentials()
    gsc_service = gbuild("searchconsole", "v1", credentials=creds)
    ga4_client = BetaAnalyticsDataClient(credentials=creds)

    # 1. Work out what is missing.
    todo = []
    for row in rows:
        key = S.row_key(row)
        if ONLY_IDS and key not in ONLY_IDS:
            continue
        if not row.get("url") or key not in history or not history[key]:
            continue
        missing = weeks_to_fill(row, history[key], start_week, last_complete)
        if missing:
            todo.append((row, key, missing))
    total_weeks = sum(len(m) for _, _, m in todo)
    print(f"{len(todo)} listing(s) have missing weeks, {total_weeks} week(s) to pull.")

    # 2. Pull real numbers. A failed week is skipped, never stored as zero.
    added = {}      # key -> [points]
    report = []     # rows for the CSV
    for n, (row, key, missing) in enumerate(todo, 1):
        urls = [row["url"]] + [a["url"] for a in (row.get("aliases") or [])]
        print(f"  [{n}/{len(todo)}] {row.get('member_name')} / {row.get('title')} ({key}): {len(missing)} week(s)")
        for ws in missing:
            try:
                point = pull_week(gsc_service, ga4_client, urls, ws)
            except Exception as e:
                print(f"    {ws}: SKIPPED ({e})")
                report.append([key, row.get("member_name"), row.get("title"), ws.isoformat(), "", "", "SKIPPED: " + str(e)[:120]])
                time.sleep(2)
                continue
            added.setdefault(key, []).append(point)
            posted_mon = monday(parse_day(row["posted_date"])) if row.get("posted_date") else None
            note = "added"
            if posted_mon and ws < posted_mon and (point["sessions"] or point["impressions"]):
                note = "added: REAL TRAFFIC BEFORE POSTED DATE, check posted_date"
            report.append([key, row.get("member_name"), row.get("title"), ws.isoformat(),
                           point["sessions"], point["impressions"], note])

    with open(REPORT_PATH, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["id", "educator", "title", "week", "visits", "impressions", "result"])
        w.writerows(report)
    n_added = sum(len(v) for v in added.values())
    n_skipped = sum(1 for r in report if str(r[6]).startswith("SKIPPED"))
    n_flag = sum(1 for r in report if "REAL TRAFFIC" in str(r[6]))
    print(f"Pulled {n_added} week(s) for {len(added)} listing(s); skipped {n_skipped}; "
          f"{n_flag} week(s) with traffic before the posted date.")

    if DRY_RUN or not added:
        print("Nothing published (dry run or nothing to add).")
        return

    # 3. Merge into a FRESH copy of history.json, so a sync that ran while the pulls
    #    were going is never overwritten. Existing points always win.
    history_file, _ = S.fetch_existing_json(S.GITHUB_HISTORY_FILE_PATH)
    fresh = (history_file or {}).get("history") or {}
    if not fresh:
        raise SystemExit("history.json came back empty on the second read; nothing was changed.")
    old_series = len(fresh)
    old_points = sum(len(v) for v in fresh.values())
    merged = {k: list(v) for k, v in fresh.items()}
    inserted = 0
    for key, points in added.items():
        if key not in merged:
            continue
        have = {str(p.get("date")) for p in merged[key]}
        for p in points:
            if p["date"] not in have:
                merged[key].append(p)
                inserted += 1
        merged[key].sort(key=lambda p: str(p.get("date", "")))
        merged[key] = merged[key][-S.HISTORY_MAX_WEEKS:]

    new_points = sum(len(v) for v in merged.values())
    if len(merged) < old_series or new_points < old_points:
        raise SystemExit(f"ABORTING: merged history is smaller than the original "
                         f"({len(merged)} vs {old_series} series, {new_points} vs {old_points} points). Nothing published.")
    print(f"Merged {inserted} new point(s). Series {old_series} -> {len(merged)}, points {old_points} -> {new_points}.")

    payload = dict(history_file)
    payload["history"] = merged
    S.publish_to_github(payload, S.GITHUB_HISTORY_FILE_PATH)
    print("history.json published.")


if __name__ == "__main__":
    sys.exit(main())
