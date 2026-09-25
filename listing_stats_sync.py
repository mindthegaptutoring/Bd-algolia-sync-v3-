"""
LWEA Listing Stats Sync
========================
Pulls per-listing search + engagement data from Google Search Console and
GA4, keyed by BD member_id, and publishes it as a single JSON file that both
the educator-facing dashboard widget and Kristen's admin triage widget can
fetch client-side (same "static JSON served from GitHub Pages" pattern
already used by lwea-search, just a second file instead of a second repo).

Designed to run on Render on the same weekly cron-job.org schedule as
bd_algolia_sync_v3.py.

BEFORE FIRST RUN — two things only Kristen can do at her laptop:
  1. Fill in the env vars listed in SETUP_GUIDE.md (BD_API_KEY plus the
     Google/GitHub ones — BD access itself now mirrors bd_algolia_sync_v3.py
     exactly, confirmed against the real script, nothing left to guess).
  2. Confirm whether a GA4 "Get Matched" / "Contact Educator" click event
     already exists (see get_ga4_stats -> CONTACT_EVENT_NAME below) — if not
     instrumented yet, that's the single highest-value thing to add before
     this data is fully useful, since it's the number that answers "is my
     listing generating leads."

Adjust TRIAGE THRESHOLDS below any time — nothing else needs to change to
retune them.
"""

import os
import json
import time
import random
import base64
import requests
from datetime import date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# BD access mirrors bd_algolia_sync_v3.py exactly: same base URL, same
# X-Api-Key header auth, same user-probing approach (BD has no bulk
# list-users/list-posts endpoint, only one-user-at-a-time lookups).
BD_BASE = "https://www.learnwitheveryavenue.com"
BD_BASE_URL = f"{BD_BASE}/api/v2"
BD_API_KEY = os.environ["BD_API_KEY"]
BD_HEADERS = {"X-Api-Key": BD_API_KEY, "Content-Type": "application/json"}

LISTING_DATA_ID = "6"   # Classes & Resources, same constant as bd_algolia_sync_v3.py
LISTING_STATUS = "1"    # published
ACTIVE_USER = "2"       # active member
MAX_USER_ID = 300       # same probe ceiling as bd_algolia_sync_v3.py — raise BOTH scripts together if membership grows past this

GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/etc/secrets/google-credentials.json")
GSC_SITE_URL = os.environ.get("GSC_SITE_URL", "https://www.learnwitheveryavenue.com/")
GA4_PROPERTY_ID = os.environ["GA4_PROPERTY_ID"]  # numeric string, e.g. "123456789"

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO = os.environ.get("GITHUB_REPO", "mindthegaptutoring/lwea-search")
GITHUB_FILE_PATH = os.environ.get("GITHUB_FILE_PATH", "listing-stats/results.json")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")

LOOKBACK_DAYS = 28
CONTACT_EVENT_NAME = "get_matched_click"  # CONFIRM this against real GA4 events; None if not instrumented yet

# ---------------------------------------------------------------------------
# TRIAGE THRESHOLDS — change these freely, logic below doesn't need to change
# ---------------------------------------------------------------------------
LOW_VISIBILITY_MAX_IMPRESSIONS = 10       # fewer than this in 28d = not being found
HIGH_IMPRESSIONS_MIN = 50                 # "enough impressions to judge CTR"
LOW_CTR_THRESHOLD = 0.02                  # below 2% CTR with decent impressions = weak title/description
LOW_ENGAGEMENT_MIN_SESSIONS = 10          # enough sessions to judge engagement
LOW_ENGAGEMENT_MAX_SECONDS = 15           # avg engagement time below this = mismatch between listing & page

# ---------------------------------------------------------------------------
# 1. Pull all listings + member IDs from BD
#    (mirrors bd_algolia_sync_v3.py's request/retry/probing logic exactly —
#    BD only exposes one-record-at-a-time lookups, no bulk list endpoint)
# ---------------------------------------------------------------------------
SESSION = requests.Session()

def bd_request(method: str, endpoint: str, *, params=None, body=None,
                max_retries: int = 5, base_delay: float = 0.5) -> dict:
    url = f"{BD_BASE_URL}{endpoint}"
    params = params or {}
    for attempt in range(max_retries):
        try:
            resp = SESSION.request(
                method=method, url=url, headers=BD_HEADERS,
                params=params, json=body, timeout=30,
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = (base_delay * (2 ** attempt)) * random.uniform(0.5, 1.5)
                print(f"  BD {resp.status_code}, retrying in {delay:.1f}s…")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            text = resp.text.strip()
            return resp.json() if text else {}
        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as e:
            delay = base_delay * (2 ** attempt)
            print(f"  Network error on {endpoint}: {e}, retrying in {delay:.1f}s…")
            time.sleep(delay)
            continue
    raise RuntimeError(f"Failed BD request {method} {endpoint} after {max_retries} attempts")


def bd_get(endpoint: str, params: dict = None) -> dict:
    return bd_request("GET", endpoint, params=params)


def get_all_active_users() -> list:
    """Probe user_id 1..MAX_USER_ID, same consecutive-miss stop logic as
    bd_algolia_sync_v3.py (BD has no bulk user list, so this is the only way in)."""
    users = []
    consecutive_misses = 0
    for uid in range(1, MAX_USER_ID + 1):
        try:
            data = bd_get("/user/get", params={"property": "user_id", "property_value": str(uid)})
            msg = data.get("message") or []
            user = msg[0] if isinstance(msg, list) and msg else None
            if user:
                consecutive_misses = 0
                sub_id = str(user.get("subscription_id", ""))
                is_active = str(user.get("active", "")) == ACTIVE_USER
                name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                if is_active and name and sub_id not in ("4", "7"):
                    users.append(user)
                time.sleep(0.5)
            else:
                consecutive_misses += 1
                if users and consecutive_misses >= 20:
                    print(f"  20 consecutive misses after user_id={uid - 1}, stopping probe.")
                    break
        except Exception as e:
            print(f"  user_id={uid} error: {e}")
            consecutive_misses += 1
            time.sleep(1.0)
    return users


def get_user_listings(user_id: str) -> list:
    """Published Classes & Resources listings for one user — same pagination
    logic as bd_algolia_sync_v3.py's get_user_listings()."""
    all_listings = []
    page_cursor = None
    while True:
        params = {"property": "user_id", "property_value": user_id}
        if page_cursor:
            params["page"] = page_cursor
        try:
            data = bd_get("/users_portfolio_groups/get", params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                break
            raise
        msg = data.get("message") or []
        if not isinstance(msg, list) or not msg:
            break
        all_listings.extend(msg)
        next_page = data.get("next_page")
        total_pages = int(data.get("total_pages") or 1)
        current = int(data.get("current_page") or 1)
        if next_page and current < total_pages:
            page_cursor = next_page
            time.sleep(0.3)
        else:
            break
    return [
        l for l in all_listings
        if str(l.get("group_status")) == LISTING_STATUS and str(l.get("data_id")) == LISTING_DATA_ID
    ]


def get_bd_listings() -> list:
    """
    Returns [{member_id, member_name, url, title, post_type}, ...] covering
    both each educator's profile page and their individual published
    Classes & Resources listings.
    """
    print("Probing BD for active educators…")
    users = get_all_active_users()
    print(f"{len(users)} active educators found")

    results = []
    for i, user in enumerate(users, 1):
        uid = str(user.get("user_id", ""))
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        filename = (user.get("filename") or "").lstrip("/")

        if filename:
            results.append({
                "member_id": uid,
                "member_name": name,
                "url": f"{BD_BASE}/{filename}",
                "title": name,
                "post_type": "profile",
            })

        print(f"  [{i}/{len(users)}] {name} (user_id={uid}) — fetching listings")
        try:
            for listing in get_user_listings(uid):
                group_filename = (listing.get("group_filename") or "").lstrip("/")
                if not group_filename:
                    continue
                results.append({
                    "member_id": uid,
                    "member_name": name,
                    "url": f"{BD_BASE}/{group_filename}",
                    "title": (listing.get("group_name") or "").strip(),
                    "post_type": "listing",
                })
        except Exception as e:
            print(f"  listings error for user_id={uid}: {e}")

        time.sleep(3.0)  # same per-user pacing as bd_algolia_sync_v3.py — keeps this under BD's ~100 req/min limit

    return results


# ---------------------------------------------------------------------------
# 2. Google auth (shared by GSC + GA4)
# ---------------------------------------------------------------------------
def get_google_credentials():
    return service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/webmasters.readonly",
            "https://www.googleapis.com/auth/analytics.readonly",
        ],
    )


# ---------------------------------------------------------------------------
# 3. Search Console: totals + top queries per URL
# ---------------------------------------------------------------------------
def get_gsc_stats(gsc_service, url):
    end = date.today() - timedelta(days=3)   # GSC data lags ~2-3 days
    start = end - timedelta(days=LOOKBACK_DAYS)

    base_body = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dimensionFilterGroups": [{
            "filters": [{"dimension": "page", "operator": "equals", "expression": url}]
        }],
    }

    # Page-level totals
    totals_resp = gsc_service.searchanalytics().query(
        siteUrl=GSC_SITE_URL, body=base_body
    ).execute()
    row = (totals_resp.get("rows") or [{}])[0]
    totals = {
        "clicks": row.get("clicks", 0),
        "impressions": row.get("impressions", 0),
        "ctr": row.get("ctr", 0.0),
        "position": round(row.get("position", 0.0), 1),
    }

    # Top 5 queries
    query_body = dict(base_body, dimensions=["query"], rowLimit=5)
    queries_resp = gsc_service.searchanalytics().query(
        siteUrl=GSC_SITE_URL, body=query_body
    ).execute()
    top_queries = [
        {"query": r["keys"][0], "clicks": r["clicks"], "impressions": r["impressions"]}
        for r in queries_resp.get("rows", [])
    ]

    return {**totals, "top_queries": top_queries}


# ---------------------------------------------------------------------------
# 4. GA4: sessions, engagement, traffic source, contact-click event
# ---------------------------------------------------------------------------
def get_ga4_stats(ga4_client, url):
    path = url if url.startswith("/") else "/" + url.split("/", 3)[-1]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS)
    date_range = DateRange(start_date=start.isoformat(), end_date=end.isoformat())
    page_filter = FilterExpression(
        filter=Filter(field_name="pagePath", string_filter=Filter.StringFilter(value=path))
    )

    # Sessions + engagement
    engagement_req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="sessions"), Metric(name="userEngagementDuration"), Metric(name="screenPageViews")],
        date_ranges=[date_range],
        dimension_filter=page_filter,
    )
    eng = ga4_client.run_report(engagement_req)
    if eng.rows:
        sessions = int(eng.rows[0].metric_values[0].value)
        total_engagement_secs = float(eng.rows[0].metric_values[1].value)
        pageviews = int(eng.rows[0].metric_values[2].value)
        avg_engagement_secs = round(total_engagement_secs / sessions, 1) if sessions else 0.0
    else:
        sessions, pageviews, avg_engagement_secs = 0, 0, 0.0

    # Traffic source breakdown
    source_req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath"), Dimension(name="sessionDefaultChannelGroup")],
        metrics=[Metric(name="sessions")],
        date_ranges=[date_range],
        dimension_filter=page_filter,
    )
    src = ga4_client.run_report(source_req)
    traffic_sources = {
        row.dimension_values[1].value: int(row.metric_values[0].value)
        for row in src.rows
    }

    # Contact/"Get Matched" click event, if instrumented
    contact_clicks = None
    if CONTACT_EVENT_NAME:
        event_filter = FilterExpression(
            filter=Filter(field_name="pagePath", string_filter=Filter.StringFilter(value=path))
        )
        event_req = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            date_ranges=[date_range],
            dimension_filter=event_filter,
        )
        ev = ga4_client.run_report(event_req)
        contact_clicks = sum(
            int(r.metric_values[0].value)
            for r in ev.rows if r.dimension_values[0].value == CONTACT_EVENT_NAME
        )

    return {
        "sessions": sessions,
        "pageviews": pageviews,
        "avg_engagement_seconds": avg_engagement_secs,
        "traffic_sources": traffic_sources,
        "contact_clicks": contact_clicks,
    }


# ---------------------------------------------------------------------------
# 5. Triage logic
# ---------------------------------------------------------------------------
def triage(gsc, ga4):
    impressions = gsc["impressions"]
    ctr = gsc["ctr"]
    sessions = ga4["sessions"]
    avg_eng = ga4["avg_engagement_seconds"]

    if impressions == 0 and sessions == 0:
        return "new_no_data"
    if impressions < LOW_VISIBILITY_MAX_IMPRESSIONS:
        return "low_visibility"          # not being found -- check title/keywords
    if impressions >= HIGH_IMPRESSIONS_MIN and ctr < LOW_CTR_THRESHOLD:
        return "high_impressions_low_ctr"  # showing up, not clicked -- title/description needs work
    if sessions >= LOW_ENGAGEMENT_MIN_SESSIONS and avg_eng < LOW_ENGAGEMENT_MAX_SECONDS:
        return "low_engagement"          # clicked, then leaves -- content/offer mismatch
    return "healthy"


# ---------------------------------------------------------------------------
# 6. Publish results.json to GitHub (same static-hosting pattern as lwea-search)
# ---------------------------------------------------------------------------
def publish_to_github(payload):
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

    # Need the current file's sha to update it (GitHub Contents API requirement)
    existing = requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH})
    sha = existing.json().get("sha") if existing.status_code == 200 else None

    content_b64 = base64.b64encode(json.dumps(payload, indent=2).encode()).decode()
    body = {
        "message": f"Update listing stats — {date.today().isoformat()}",
        "content": content_b64,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha

    put_resp = requests.put(api_url, headers=headers, json=body)
    put_resp.raise_for_status()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    creds = get_google_credentials()
    gsc_service = gbuild("searchconsole", "v1", credentials=creds)
    ga4_client = BetaAnalyticsDataClient(credentials=creds)

    listings = get_bd_listings()
    results = []

    for listing in listings:
        try:
            gsc = get_gsc_stats(gsc_service, listing["url"])
            ga4 = get_ga4_stats(ga4_client, listing["url"])
            flag = triage(gsc, ga4)
            results.append({**listing, "gsc": gsc, "ga4": ga4, "triage_flag": flag})
        except Exception as e:
            # One bad URL shouldn't kill the whole run
            results.append({**listing, "error": str(e)})

    payload = {
        "generated_at": date.today().isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "listings": results,
    }
    publish_to_github(payload)
    print(f"Synced {len(results)} listings.")


if __name__ == "__main__":
    main()
