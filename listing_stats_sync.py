"""
LWEA Listing Stats Sync
========================
Version: 1.5.0
  1.0.0 — initial working version: BD user-probing, GSC + GA4 per-listing pull,
          triage flags, GitHub-published results.json
  1.1.0 — corrected CONTACT_EVENT_NAME to the real, confirmed-live "mailto_click"
          event (was a guessed placeholder); added connect_pageviews for BD's
          native "Send Message" flow (tracked as a /connect pageview, not a
          click event); switched Google auth from an env-var JSON string to a
          Render Secret File
  1.2.0 — added outbound-click breakdown by domain (Visit Website / Read Google
          Reviews / etc.), using GA4 Enhanced Measurement's existing "Outbound
          clicks" tracking — requires the "Link Domain" custom dimension to be
          registered in GA4 Admin first (see SETUP_GUIDE.md); does not backfill
  1.3.0 — switched triage from fixed absolute thresholds to percentile-based
          cutoffs computed fresh each run, separately for profiles vs listings.
          Fixed thresholds were sized for a much bigger site (max real
          impressions ever seen: 23, vs. a 10-impression floor and a
          50-impression ceiling in the old logic) and caught 89% of listings
          in a single "low_visibility" bucket regardless of actual relative
          performance. main() is now two passes: collect all data, then
          compute + apply thresholds, so the cutoffs reflect this run's real
          distribution instead of a guess.
  1.4.0 — added trending. results.json is UNCHANGED — same shape as before,
          so Widget 53's existing snapshot table and Widget 54's admin triage
          table need zero changes and carry zero risk from this update. A
          NEW, separate file (history.json) is published alongside it: a
          dict of {listing_url: [weekly_snapshot, ...]}, one entry appended
          each week, capped at HISTORY_MAX_WEEKS (52 = ~1 year). Kept as a
          separate file on purpose — bundling 52 weeks of history into every
          listing inside results.json would roughly 10x that file's size and
          make every dashboard load (which only ever needs the CURRENT
          snapshot) pay for history nobody's viewing yet. Only the new trend
          chart in Widget 53 needs to fetch history.json, and only when it's
          actually rendering a chart.
          Before this file has any real weekly data in it, run
          backfill_history.py once to seed up to ~12 months of real history
          from GSC + GA4's own retained data, rather than waiting a year for
          the chart to have anything to show.
  1.4.1 — fixed a real unit mismatch caught during testing: make_history_point
          was reading from the same gsc/ga4 values as the snapshot table —
          28-day rolling totals — and writing them into history.json as if
          they were single-week figures. backfill_history.py's points are
          true 7-day totals. Mixing the two in one trend array would have
          made every point silently ~4x too high the moment live data picked
          up from backfilled data, a fake jump right at the seam with no
          error or warning. get_gsc_stats and get_ga4_stats now take an
          optional `days` argument; main() does a second, 7-day-only pull
          per listing (include_queries=False on the GSC side, since history
          doesn't need top_queries) specifically for history.json, leaving
          the 28-day snapshot pull and results.json's schema untouched. Adds
          two GSC/GA4 calls per listing to the regular weekly run.
  1.5.0 — added `posted_date` to every listing in results.json (profiles:
          signup date; Classes & Resources listings: cover-photo upload date
          as the closest available creation-date proxy — this API surface
          has no clean "created" column). Purely additive to results.json's
          schema, existing widgets ignore the new field. Lets the dashboard
          mark each listing's actual creation date on its trend sparkline
          instead of implying the chart's start date IS the creation date,
          which isn't always true (GSC/GA4 indexing lag can mean the first
          real data point comes after the listing already existed).
          UNVERIFIED: the exact field this script's own API auth context
          returns for a listing's cover-photo date hasn't been confirmed —
          only confirmed against a separate admin-tooling response shape.
          Check results.json after the first run post-deploy; if
          posted_date is null for every listing row, the field path in
          _extract_posted_date_iso() needs adjusting to whatever this
          endpoint actually returns.
Pulls per-listing search + engagement data from Google Search Console and
GA4, keyed by BD member_id, and publishes it as JSON files that both the
educator-facing dashboard widget and Kristen's admin triage widget can fetch
client-side (same "static JSON served from GitHub Pages" pattern already
used by lwea-search, just additional files instead of a second repo).

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
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter, FilterExpressionList
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
GITHUB_HISTORY_FILE_PATH = os.environ.get("GITHUB_HISTORY_FILE_PATH", "listing-stats/history.json")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")

LOOKBACK_DAYS = 28
HISTORY_MAX_WEEKS = 52  # rolling ~1 year of weekly snapshots per listing before oldest points age out
CONTACT_EVENT_NAME = "mailto_click"  # confirmed live sitewide — fires on any a[href^="mailto:"] click, anywhere on the page
TRACK_CONNECT_PAGEVIEWS = True  # BD's native "Send Message" button has no click event, just navigates to <profile_url>/connect — pull as a pageview instead
TRACK_OUTBOUND_CLICKS = True  # requires the "Link Domain" custom dimension registered in GA4 Admin (see SETUP_GUIDE.md) — safe to leave True even before that's done, the call just fails gracefully per-listing until then

# ---------------------------------------------------------------------------
# TRIAGE THRESHOLDS — percentile-based, computed fresh from THIS run's own
# data (see compute_thresholds below), not fixed numbers. Profile pages and
# Classes & Resources listings get their own separate cutoffs, since they
# have different baseline traffic patterns and shouldn't be judged against
# each other. Change the percentiles below any time — nothing else needs to
# change to retune them, and they self-adjust as the site's traffic grows
# instead of needing periodic manual recalibration.
# ---------------------------------------------------------------------------
LOW_VISIBILITY_PERCENTILE = 25    # bottom this % of impressions, within post_type = low_visibility
HIGH_VISIBILITY_PERCENTILE = 75   # top this % of impressions, within post_type = "enough traffic to judge CTR on"
LOW_CTR_PERCENTILE = 25           # bottom this % of CTR (among listings with any impressions) = weak title/description
LOW_ENGAGEMENT_PERCENTILE = 25    # bottom this % of avg engagement (among listings with any sessions) = content/offer mismatch

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


def _extract_posted_date_iso(listing: dict):
    """
    Best-effort creation-date proxy for a Classes & Resources listing.
    users_portfolio_groups has no clean "created" column on this API surface
    (confirmed against the admin tooling's response shape — group_date is
    always null). The closest real proxy is the cover photo's date_added,
    since photos are normally uploaded at listing-creation time. Tries a
    couple of plausible field shapes defensively; returns None (not a guess)
    if none are present, since this API endpoint's exact shape for this
    field hasn't been confirmed yet from the sync script's own auth context
    — verify after first deploy that this is actually populating, and adjust
    the field path here if BD returns something different than expected.
    """
    portfolio = listing.get("users_portfolio") or {}
    if isinstance(portfolio, list) and portfolio:
        portfolio = portfolio[0]
    raw = portfolio.get("photo_date_added") if isinstance(portfolio, dict) else None
    if not raw:
        raw = listing.get("group_date") or listing.get("date_created")
    if not raw or len(str(raw)) < 8:
        return None
    raw = str(raw)
    try:
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    except Exception:
        return None


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
                "posted_date": (user.get("signup_date") or "")[:10] or None,  # BD returns ISO datetime, take just the date part
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
                    "posted_date": _extract_posted_date_iso(listing),
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
def get_gsc_stats(gsc_service, url, days=LOOKBACK_DAYS, include_queries=True):
    end = date.today() - timedelta(days=3)   # GSC data lags ~2-3 days
    start = end - timedelta(days=days)

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

    top_queries = []
    if include_queries:
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
def get_connect_pageviews(ga4_client, profile_url):
    """
    BD's native 'Send Message' button has no click event — it just navigates to
    <profile_url>/connect. Pull that page's own pageview count as the proxy for
    'someone used the native BD contact flow'. Only meaningful for profile-type
    URLs (unconfirmed whether individual Classes & Resources listings have their
    own /connect sub-path or route through the profile's — verify before trusting
    this for post_type == "listing").
    """
    path = "/" + profile_url.split("/", 3)[-1].rstrip("/") + "/connect"
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS)
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        dimension_filter=FilterExpression(
            filter=Filter(field_name="pagePath", string_filter=Filter.StringFilter(value=path))
        ),
    )
    resp = ga4_client.run_report(req)
    return int(resp.rows[0].metric_values[0].value) if resp.rows else 0


def get_outbound_click_stats(ga4_client, url):
    """
    Breaks down GA4 Enhanced Measurement's automatic "Outbound clicks" events
    by destination domain for one page — e.g. {"mindthegaptutoring.ca": 4,
    "google.com": 1} — which is exactly the Visit Website vs. Read Google
    Reviews vs. anything-else split needed to test the "do parents skip BD's
    contact flow for the educator's own site" question.

    Requires the "Link Domain" custom dimension (event-scoped, mapped to the
    link_domain event parameter) to be registered in GA4 Admin first — see
    SETUP_GUIDE.md. Until it's registered, GA4's API rejects the dimension
    name outright; the caller catches that and treats it as "not ready yet"
    rather than a real error.
    """
    path = url if url.startswith("/") else "/" + url.split("/", 3)[-1]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS)
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="customEvent:link_domain")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        dimension_filter=FilterExpression(
            and_group=FilterExpressionList(expressions=[
                FilterExpression(filter=Filter(field_name="pagePath", string_filter=Filter.StringFilter(value=path))),
                FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value="click"))),
            ])
        ),
    )
    resp = ga4_client.run_report(req)
    return {row.dimension_values[0].value: int(row.metric_values[0].value) for row in resp.rows}


def get_ga4_stats(ga4_client, url, days=LOOKBACK_DAYS):
    path = url if url.startswith("/") else "/" + url.split("/", 3)[-1]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days)
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
# 5. Triage logic — percentile-based, computed fresh from this run's data
# ---------------------------------------------------------------------------
def percentile(sorted_values, p):
    """Nearest-rank percentile on an already-sorted list. p is 0-100."""
    if not sorted_values:
        return 0
    idx = min(int(len(sorted_values) * p / 100), len(sorted_values) - 1)
    return sorted_values[idx]


def compute_thresholds(results):
    """
    One set of percentile cutoffs per post_type (profile vs listing),
    computed from this run's own data only — not shared across the two
    groups, and not carried over between runs. This is what makes the
    triage flag stay meaningful as the site's traffic grows: the cutoffs
    move with the site instead of being fixed numbers that need periodic
    manual retuning.
    """
    thresholds = {}
    for post_type in ("profile", "listing"):
        group = [r for r in results if r.get("post_type") == post_type and "gsc" in r and "ga4" in r]
        impressions = sorted(r["gsc"]["impressions"] for r in group)
        ctrs = sorted(r["gsc"]["ctr"] for r in group if r["gsc"]["impressions"] > 0)
        sessions = sorted(r["ga4"]["sessions"] for r in group)
        engagements = sorted(r["ga4"]["avg_engagement_seconds"] for r in group if r["ga4"]["sessions"] > 0)
        thresholds[post_type] = {
            "low_visibility_max": percentile(impressions, LOW_VISIBILITY_PERCENTILE),
            "high_visibility_min": percentile(impressions, HIGH_VISIBILITY_PERCENTILE),
            "low_ctr_max": percentile(ctrs, LOW_CTR_PERCENTILE),
            "meaningful_sessions_min": percentile(sessions, 50),
            "low_engagement_max": percentile(engagements, LOW_ENGAGEMENT_PERCENTILE),
            "sample_size": len(group),
        }
    return thresholds


def triage(gsc, ga4, t):
    impressions = gsc["impressions"]
    ctr = gsc["ctr"]
    sessions = ga4["sessions"]
    avg_eng = ga4["avg_engagement_seconds"]

    if impressions == 0 and sessions == 0:
        return "new_no_data"
    if impressions <= t["low_visibility_max"]:
        return "low_visibility"          # bottom of this post_type's own range -- check title/keywords
    if impressions >= t["high_visibility_min"] and ctr <= t["low_ctr_max"]:
        return "high_impressions_low_ctr"  # showing up more than most peers, clicked less -- title/description needs work
    if sessions >= t["meaningful_sessions_min"] and avg_eng <= t["low_engagement_max"]:
        return "low_engagement"          # clicked, then leaves faster than most peers -- content/offer mismatch
    return "healthy"


# ---------------------------------------------------------------------------
# 6. History (trending) — NEW in 1.4.0
# ---------------------------------------------------------------------------
def make_history_point(r):
    """One compact weekly snapshot for the trend chart. Deliberately lean —
    only what a trend line actually needs, not the full result row (no
    top_queries, no traffic_sources breakdown — those stay snapshot-only).

    IMPORTANT: reads from r["_gsc_week"] / r["_ga4_week"] — a separate
    7-day-only pull done alongside the normal 28-day snapshot pull (see
    main()), NOT from r["gsc"] / r["ga4"], which are 28-day rolling totals.
    Mixing a 28-day total into the same trend array as backfill_history.py's
    true 7-day weekly buckets would make every point look ~4x too high
    right where live data picks up from backfilled data — same number,
    different unit, silently wrong."""
    gsc = r.get("_gsc_week", {})
    ga4 = r.get("_ga4_week", {})
    return {
        "date": date.today().isoformat(),
        "impressions": gsc.get("impressions", 0),
        "clicks": gsc.get("clicks", 0),
        "ctr": gsc.get("ctr", 0.0),
        "position": gsc.get("position", 0.0),
        "sessions": ga4.get("sessions", 0),
        "avg_engagement_seconds": ga4.get("avg_engagement_seconds", 0.0),
        "contact_clicks": ga4.get("contact_clicks"),
        "connect_pageviews": ga4.get("connect_pageviews"),
        "triage_flag": r.get("triage_flag", "error"),
    }


def build_updated_history(results, existing_history_index):
    """
    existing_history_index: {url: [weekly_point, ...]} from the last published
    history.json (empty dict if this is the first run with trending, or the
    file didn't exist / failed to parse).
    Returns the new full index, one point appended per listing this run,
    each list capped at HISTORY_MAX_WEEKS.
    """
    new_index = {}
    for r in results:
        url = r.get("url")
        if not url:
            continue
        prior = list(existing_history_index.get(url, []))
        if "gsc" in r and "ga4" in r:
            prior.append(make_history_point(r))
        new_index[url] = prior[-HISTORY_MAX_WEEKS:]
    return new_index


# ---------------------------------------------------------------------------
# 7. GitHub read/publish — generalized in 1.4.0 to take a file_path, since
#    two files are now published each run instead of one
# ---------------------------------------------------------------------------
def fetch_existing_json(file_path):
    """
    GET the currently-published JSON at file_path (content + sha), so a run
    can build on top of it instead of starting fresh. Returns
    (parsed_json_or_None, sha_or_None). A missing file, a network hiccup, or
    unparseable content all just fall back to None — callers treat that as
    "start from scratch", never as a fatal error.
    """
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    resp = requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH})
    if resp.status_code != 200:
        return None, None
    data = resp.json()
    sha = data.get("sha")
    try:
        content = base64.b64decode(data["content"]).decode("utf-8")
        return json.loads(content), sha
    except Exception as e:
        print(f"  Could not parse existing {file_path} ({e}) — starting fresh.")
        return None, sha


def publish_to_github(payload, file_path):
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

    # Need the current file's sha to update it (GitHub Contents API requirement)
    existing = requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH})
    sha = existing.json().get("sha") if existing.status_code == 200 else None

    content_b64 = base64.b64encode(json.dumps(payload, indent=2).encode()).decode()
    body = {
        "message": f"Update {file_path} — {date.today().isoformat()}",
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

    # Pass 1: collect every listing's raw GSC/GA4 data. No triage flag yet --
    # the percentile cutoffs below can only be computed once this run's full
    # distribution is known.
    for listing in listings:
        try:
            gsc = get_gsc_stats(gsc_service, listing["url"])
            ga4 = get_ga4_stats(ga4_client, listing["url"])
            if TRACK_CONNECT_PAGEVIEWS and listing["post_type"] == "profile":
                ga4["connect_pageviews"] = get_connect_pageviews(ga4_client, listing["url"])
            else:
                ga4["connect_pageviews"] = None  # not yet confirmed whether listing pages have their own /connect path
            if TRACK_OUTBOUND_CLICKS:
                try:
                    ga4["outbound_clicks_by_domain"] = get_outbound_click_stats(ga4_client, listing["url"])
                except Exception as e:
                    ga4["outbound_clicks_by_domain"] = None  # "Link Domain" custom dimension likely not registered yet
                    print(f"  outbound click breakdown unavailable ({e}) — register the Link Domain custom dimension in GA4 Admin")
            else:
                ga4["outbound_clicks_by_domain"] = None

            # Separate 7-day-only pull, for history.json ONLY — see
            # make_history_point's docstring for why this can't reuse the
            # 28-day gsc/ga4 above. include_queries=False since history
            # doesn't need top_queries, saving one GSC call per listing.
            gsc_week = get_gsc_stats(gsc_service, listing["url"], days=7, include_queries=False)
            ga4_week = get_ga4_stats(ga4_client, listing["url"], days=7)

            results.append({**listing, "gsc": gsc, "ga4": ga4, "_gsc_week": gsc_week, "_ga4_week": ga4_week})
        except Exception as e:
            # One bad URL shouldn't kill the whole run
            results.append({**listing, "error": str(e)})

    # Pass 2: compute this run's own percentile cutoffs, then apply them.
    thresholds = compute_thresholds(results)
    print(f"  Triage thresholds this run — profile: {thresholds['profile']}, listing: {thresholds['listing']}")
    for r in results:
        if "gsc" in r and "ga4" in r:
            r["triage_flag"] = triage(r["gsc"], r["ga4"], thresholds[r["post_type"]])
        else:
            r["triage_flag"] = "error"

    # Build history.json's data from the full result rows (still carrying
    # _gsc_week/_ga4_week) BEFORE those internal-only fields get stripped
    # below for the public results.json payload.
    existing_history, _ = fetch_existing_json(GITHUB_HISTORY_FILE_PATH)
    existing_history_index = (existing_history or {}).get("history", {})
    new_history_index = build_updated_history(results, existing_history_index)

    # results.json — UNCHANGED shape from 1.3.0. Existing widgets (Widget 53's
    # snapshot table, Widget 54's admin triage table) need no changes and
    # carry zero risk from this release. Strip the internal history-only
    # fields so this promise actually holds — they were never part of this
    # file's schema.
    for r in results:
        r.pop("_gsc_week", None)
        r.pop("_ga4_week", None)
    payload = {
        "generated_at": date.today().isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "triage_thresholds": thresholds,  # published for transparency — the exact cutoffs this run used, by post_type
        "listings": results,
    }
    publish_to_github(payload, GITHUB_FILE_PATH)

    # history.json — NEW in 1.4.0. Separate file so results.json (and every
    # existing widget reading it) stays exactly as fast and small as before.
    history_payload = {
        "generated_at": date.today().isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "history_max_weeks": HISTORY_MAX_WEEKS,
        "history": new_history_index,  # {listing_url: [weekly_point, ...]}
    }
    publish_to_github(history_payload, GITHUB_HISTORY_FILE_PATH)

    print(f"Synced {len(results)} listings. History now covers up to {HISTORY_MAX_WEEKS} weeks per listing.")


if __name__ == "__main__":
    main()
