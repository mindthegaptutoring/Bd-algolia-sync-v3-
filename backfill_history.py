"""
LWEA Listing Stats — One-Time History Backfill
================================================
Run ONCE, manually, before the trending chart goes live to educators. Not
part of the weekly cron schedule — this is a separate script you run by hand
from your laptop (or a one-off Render job) after deploying listing_stats_sync.py
v1.4.0.

WHY THIS EXISTS: history.json only starts filling in from whenever
listing_stats_sync.py v1.4.0 first runs. Without this script, the trend chart
would be an empty line for the first 52 weeks. GSC keeps ~16 months of data
queryable on demand, and GA4 keeps its default retention window too, so this
pulls what already exists rather than making educators wait a year.

WHAT IT DOES NOT BACKFILL: contact_clicks (mailto_click), connect_pageviews,
and outbound_clicks_by_domain are left as null for every backfilled week.
Those events were only instrumented recently (see listing_stats_sync.py's
version history — v1.1.0 and v1.2.0), so older weeks genuinely have no data
for them, not zero-because-nobody-clicked. Showing null instead of 0 keeps
that distinction honest. The regular weekly sync fills these in normally
going forward.

HOW IT'S EFFICIENT: rather than one API call per listing per week (which
would multiply out to thousands of calls), this makes ONE GSC call and ONE
GA4 call per listing, each returning daily-granularity rows for the whole
backfill window, then buckets those days into ISO weeks in Python. Same
order of magnitude of API calls as one regular weekly sync run, not 52x it.

SAFE TO RE-RUN: only fills in weeks that don't already exist in history.json
for a given listing. Never overwrites a week that's already there (whether
from a prior backfill run or from the regular weekly sync), so re-running
this after the trending feature has been live for a while just tops up any
listing that's new since the last backfill, without disturbing real data.

Usage:
    BACKFILL_WEEKS=52 python backfill_history.py
    (defaults to 52 weeks / ~1 year if not set — reduce if you'd rather
    backfill less, e.g. BACKFILL_WEEKS=12 for just the last quarter)
"""

import os
import json
from datetime import date, timedelta
from collections import defaultdict

from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter

from listing_stats_sync import (
    get_google_credentials, get_bd_listings, fetch_existing_json, publish_to_github,
    GSC_SITE_URL, GA4_PROPERTY_ID, GITHUB_HISTORY_FILE_PATH, HISTORY_MAX_WEEKS,
)
from googleapiclient.discovery import build as gbuild
from google.analytics.data_v1beta import BetaAnalyticsDataClient

BACKFILL_WEEKS = int(os.environ.get("BACKFILL_WEEKS", "52"))


def week_start(d: date) -> date:
    """Monday of the ISO week containing d — the bucket key used throughout."""
    return d - timedelta(days=d.weekday())


def backfill_gsc_by_week(gsc_service, url, start: date, end: date) -> dict:
    """
    ONE GSC call for the whole window, dimensioned by date, then bucketed
    into weeks here. Returns {week_start_iso: {"impressions", "clicks", "ctr", "position"}}.
    """
    body = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dimensions": ["date"],
        "dimensionFilterGroups": [{
            "filters": [{"dimension": "page", "operator": "equals", "expression": url}]
        }],
        "rowLimit": 25000,  # GSC max — comfortably covers a year of daily rows for one URL
    }
    resp = gsc_service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=body).execute()

    buckets = defaultdict(lambda: {"impressions": 0, "clicks": 0, "position_weighted_sum": 0.0})
    for row in resp.get("rows", []):
        day = date.fromisoformat(row["keys"][0])
        wk = week_start(day).isoformat()
        impressions = row.get("impressions", 0)
        buckets[wk]["impressions"] += impressions
        buckets[wk]["clicks"] += row.get("clicks", 0)
        buckets[wk]["position_weighted_sum"] += row.get("position", 0.0) * impressions

    result = {}
    for wk, b in buckets.items():
        impressions = b["impressions"]
        result[wk] = {
            "impressions": impressions,
            "clicks": b["clicks"],
            "ctr": round(b["clicks"] / impressions, 4) if impressions else 0.0,
            "position": round(b["position_weighted_sum"] / impressions, 1) if impressions else 0.0,
        }
    return result


def backfill_ga4_by_week(ga4_client, url, start: date, end: date) -> dict:
    """
    ONE GA4 call for the whole window, dimensioned by pagePath + date, then
    bucketed into weeks here. Returns
    {week_start_iso: {"sessions", "avg_engagement_seconds"}}.
    """
    path = url if url.startswith("/") else "/" + url.split("/", 3)[-1]
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath"), Dimension(name="date")],
        metrics=[Metric(name="sessions"), Metric(name="userEngagementDuration")],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        dimension_filter=FilterExpression(
            filter=Filter(field_name="pagePath", string_filter=Filter.StringFilter(value=path))
        ),
    )
    resp = ga4_client.run_report(req)

    buckets = defaultdict(lambda: {"sessions": 0, "engagement_secs": 0.0})
    for row in resp.rows:
        # GA4's "date" dimension comes back as YYYYMMDD, not ISO — parse accordingly
        day_str = row.dimension_values[1].value
        day = date(int(day_str[0:4]), int(day_str[4:6]), int(day_str[6:8]))
        wk = week_start(day).isoformat()
        buckets[wk]["sessions"] += int(row.metric_values[0].value)
        buckets[wk]["engagement_secs"] += float(row.metric_values[1].value)

    result = {}
    for wk, b in buckets.items():
        sessions = b["sessions"]
        result[wk] = {
            "sessions": sessions,
            "avg_engagement_seconds": round(b["engagement_secs"] / sessions, 1) if sessions else 0.0,
        }
    return result


def main():
    creds = get_google_credentials()
    gsc_service = gbuild("searchconsole", "v1", credentials=creds)
    ga4_client = BetaAnalyticsDataClient(credentials=creds)

    print(f"Backfilling up to {BACKFILL_WEEKS} weeks of history…")
    listings = get_bd_listings()
    print(f"{len(listings)} listings to backfill")

    existing_history, _ = fetch_existing_json(GITHUB_HISTORY_FILE_PATH)
    history_index = dict((existing_history or {}).get("history", {}))

    end = date.today() - timedelta(days=3)  # match the regular sync's GSC lag buffer
    start = end - timedelta(weeks=BACKFILL_WEEKS)

    for i, listing in enumerate(listings, 1):
        url = listing["url"]
        print(f"  [{i}/{len(listings)}] {listing['title']}")
        try:
            gsc_weeks = backfill_gsc_by_week(gsc_service, url, start, end)
        except Exception as e:
            print(f"    GSC backfill failed for {url}: {e}")
            gsc_weeks = {}
        try:
            ga4_weeks = backfill_ga4_by_week(ga4_client, url, start, end)
        except Exception as e:
            print(f"    GA4 backfill failed for {url}: {e}")
            ga4_weeks = {}

        existing_weeks = {pt["date"] for pt in history_index.get(url, [])}
        all_week_keys = sorted(set(gsc_weeks) | set(ga4_weeks))
        new_points = []
        for wk in all_week_keys:
            if wk in existing_weeks:
                continue  # never overwrite a week that's already there — see module docstring
            g = gsc_weeks.get(wk, {"impressions": 0, "clicks": 0, "ctr": 0.0, "position": 0.0})
            a = ga4_weeks.get(wk, {"sessions": 0, "avg_engagement_seconds": 0.0})
            new_points.append({
                "date": wk,
                "impressions": g["impressions"],
                "clicks": g["clicks"],
                "ctr": g["ctr"],
                "position": g["position"],
                "sessions": a["sessions"],
                "avg_engagement_seconds": a["avg_engagement_seconds"],
                "contact_clicks": None,       # not backfilled — see module docstring
                "connect_pageviews": None,    # not backfilled — see module docstring
                "triage_flag": None,          # triage is a same-run comparison, doesn't apply retroactively to a single listing
            })

        merged = sorted(
            history_index.get(url, []) + new_points,
            key=lambda pt: pt["date"],
        )[-HISTORY_MAX_WEEKS:]
        history_index[url] = merged
        print(f"    +{len(new_points)} weeks backfilled ({len(merged)} total on file)")

    history_payload = {
        "generated_at": date.today().isoformat(),
        "backfilled_at": date.today().isoformat(),
        "history_max_weeks": HISTORY_MAX_WEEKS,
        "history": history_index,
    }
    publish_to_github(history_payload, GITHUB_HISTORY_FILE_PATH)
    print("Backfill complete — history.json published.")


if __name__ == "__main__":
    main()
