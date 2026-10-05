"""
LWEA Listing Stats Sync
========================
Version: 1.5.6
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
  1.5.1 — fixed a bug that silently wiped every listing's trend history on
          2026-09-28. fetch_existing_json() only knew how to read GitHub's
          Contents API when it returns file content inline, which only
          happens under 1 MiB (1,048,576 bytes). history.json crossed that
          line (1,072,223 bytes) and every subsequent fetch got a 200
          response with no `content` field, which the old code treated as
          "nothing to parse, start fresh" — so every listing's whole
          history reset to a single point that run, all at once, with no
          error surfaced anywhere a person would see it. Now falls back to
          the `download_url` GitHub still provides for large files, which
          has no such size limit. This does NOT restore the history lost
          on 2026-09-28 — that data may still be recoverable from the
          `mindthegaptutoring/lwea-search` repo's git commit history for
          that file path, since publish_to_github overwrites the file's
          current content but git itself keeps prior versions unless
          history was force-pushed or squashed. Worth checking before
          concluding it's gone for good.
  1.5.2 — fixed a real GA4-side data loss bug for any listing whose URL
          slug contains a percent-encoded non-ASCII character (e.g. an
          en-dash, "grades-3–12" stored as the literal text
          "%E2%80%9312" in BD's group_filename). Every GA4 query in this
          script built its pagePath filter directly from that still-
          encoded stored URL, but browsers decode the URL before
          reporting document.location.pathname to GA4 — so GA4's real
          pagePath contains the actual "–" character, never the encoded
          text, and the exact-match filter silently matched nothing.
          Confirmed live: a listing with a verified real mailto_click
          event in GA4's own UI showed zero for every GA4 field —
          sessions, engagement, contact_clicks, outbound clicks, all of
          it, not just the one metric that surfaced this. Added a shared
          bd_url_to_ga4_path() helper (unquote() the path before use) and
          applied it everywhere a GA4 pagePath filter is built, including
          backfill_history.py's own copy of this logic. Does not affect
          GSC's queries — those use the full URL against Search Console's
          own "page" dimension, a different matching mechanism, unrelated
          to this bug. Any currently-published listing with a non-ASCII
          slug character has been under-reporting GA4 data (possibly to
          zero) since this script's very first version — this isn't new
          breakage, it's a bug that's been there the whole time and only
          surfaced now because we happened to cross-check one listing's
          numbers against GA4's own UI directly.
  1.5.3 — fixed history.json duplicating a week's data point instead of
          replacing it when the sync gets run more than once in the same
          week (e.g. a manual re-run to verify a fix — exactly what
          surfaced this, on 2026-09-28). make_history_point() used to
          stamp every point with date.today(), and build_updated_history()
          always appended, with no check for an existing point that same
          week. Since each point is a rolling 7-day window, two points
          close together in a re-run scenario overlap heavily in which
          real days they cover — visually and numerically double-counting
          substantially the same week rather than showing two genuinely
          separate weeks. Both functions now key on week_start() (the
          Monday of the current week, not the exact run date) and
          build_updated_history replaces any existing point for that week
          instead of appending a second one. week_start() itself moved
          here from backfill_history.py, which now imports it from this
          file instead of keeping its own copy — two copies of "what
          counts as the same week" was exactly the kind of thing that
          could quietly drift apart between the two scripts, so there's
          now one definition both share.
  1.5.4 — fixed real double-counting in the weekly history pull, found by
          Kristen comparing the dashboard against the raw history.json
          directly: a listing with zero impressions for 20+ straight weeks
          got 3 real impressions, and BOTH the Sept 21 and Sept 28 history
          points showed those same 3 impressions. 1.5.3 fixed how a point
          gets *labeled and stored* (Monday-of-week, replace not append)
          but never fixed what window of data gets *queried* for that
          point — get_gsc_stats/get_ga4_stats's weekly call still used
          "the 7 days ending near whenever the script happens to run,"
          not a fixed calendar week. Two runs a few days apart (a
          scheduled Monday run and a manual re-run days later, say) had
          overlapping trailing windows and could both capture the same
          real days' activity. Both functions now take optional
          start_override/end_override so a caller can pin them to an
          exact range; main() computes this_week_start/end once per run
          (Monday through Sunday of the current week, clamped to each
          API's own reporting lag) and passes that to the weekly pull
          instead of a day-count. A week that hasn't started yet within
          an API's lag window correctly returns zeros rather than
          reaching backward into the prior week to fill the gap.
  1.5.5 — added fetch_and_publish_reviews(), publishing reviews.json
          alongside results.json/history.json. Feeds Widget 55
          (LWEA - Listing Reviews), which shows an educator's real,
          approved reviews on their Classes & Resources listing pages
          without them needing to paste testimonial text into every
          listing — the exact habit behind several duplicate-content
          problems found and fixed this session. NOT wired into this
          file's own main() — see the function's docstring — reviews
          need a much faster refresh cadence than this script's weekly
          schedule, so a separate script (reviews_sync.py) calls this
          function on its own frequent cron instead. UNVERIFIED: the
          endpoint path (/users_reviews/get) is a naming-convention
          guess, not confirmed against this script's own auth context —
          wrapped in its own try/except so a wrong guess fails quietly
          rather than taking anything else down with it.
  1.5.6 — fetch_and_publish_reviews() now skips the GitHub write when the
          approved reviews are identical to what's already published
          (generated_at is excluded from the comparison, since it changes
          daily regardless). Without this, a frequently-scheduled reviews
          job commits an identical reviews.json every run. Each
          educator's reviews are also now sorted newest-first before
          publishing: Widget 55 shows the first five in file order, so
          this is what makes it show the newest five, and a stable order
          is what lets the unchanged-check work reliably.
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
from urllib.parse import unquote

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
def get_gsc_stats(gsc_service, url, days=LOOKBACK_DAYS, include_queries=True, start_override=None, end_override=None):
    """
    start_override/end_override let a caller pin this to an exact calendar
    range instead of "N days ending near today" — see main()'s weekly pull,
    where this matters: a trailing window drifts with whenever the script
    happens to run, so two runs a few days apart can both capture the same
    underlying days, double-counting real activity across two "weekly"
    history points. A fixed calendar week can't overlap its neighbors no
    matter when in the week the script actually runs.
    """
    end = end_override if end_override is not None else (date.today() - timedelta(days=3))   # GSC data lags ~2-3 days
    start = start_override if start_override is not None else (end - timedelta(days=days))
    if start > end:
        # Too early in the current week for GSC's lag to have any data yet
        # — a real "nothing to report" state, not an error.
        return {"clicks": 0, "impressions": 0, "ctr": 0.0, "position": 0.0, "top_queries": []}

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
def bd_url_to_ga4_path(url: str) -> str:
    """
    Converts a full BD URL (or an already-relative path) into the path
    GA4's `pagePath` dimension will actually contain.

    IMPORTANT: BD's group_filename (and therefore every listing's stored
    `url`) comes back percent-encoded for any non-ASCII character — e.g. an
    en-dash in a slug like "grades-3–12" is stored as the literal text
    "%E2%80%9312". Browsers decode that automatically when reporting
    document.location.pathname to GA4, so GA4's real pagePath value
    contains the actual "–" character, not the percent-encoded text. A
    filter built from the raw, still-encoded url therefore does an exact
    string match against something that never actually appears in GA4's
    data — silently zero rows, not an error. Confirmed live: an en-dash
    listing (Zee Roda's "Online Math Tutor... Grades 3–12") had real,
    verified mailto_click events in GA4's own UI that this script reported
    as zero, and its entire GA4 block (sessions, engagement, everything)
    was zero for the same reason, not just contact clicks. unquote()
    fixes this for every GA4 query in this script, not just one.
    """
    raw = url if url.startswith("/") else "/" + url.split("/", 3)[-1]
    return unquote(raw)


def get_connect_pageviews(ga4_client, profile_url):
    """
    BD's native 'Send Message' button has no click event — it just navigates to
    <profile_url>/connect. Pull that page's own pageview count as the proxy for
    'someone used the native BD contact flow'. Only meaningful for profile-type
    URLs (unconfirmed whether individual Classes & Resources listings have their
    own /connect sub-path or route through the profile's — verify before trusting
    this for post_type == "listing").
    """
    path = bd_url_to_ga4_path(profile_url).rstrip("/") + "/connect"
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
    path = bd_url_to_ga4_path(url)
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


def get_ga4_stats(ga4_client, url, days=LOOKBACK_DAYS, start_override=None, end_override=None):
    """See get_gsc_stats's docstring for why start_override/end_override
    exist — same fixed-calendar-week reasoning applies here."""
    path = bd_url_to_ga4_path(url)
    end = end_override if end_override is not None else (date.today() - timedelta(days=1))
    start = start_override if start_override is not None else (end - timedelta(days=days))
    if start > end:
        return {
            "sessions": 0, "pageviews": 0, "avg_engagement_seconds": 0.0,
            "traffic_sources": {}, "contact_clicks": 0 if CONTACT_EVENT_NAME else None,
        }
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
def week_start(d: date) -> date:
    """
    Monday of the ISO week containing d — the single bucket key used
    everywhere in the history system (this file's make_history_point,
    and backfill_history.py, which imports this instead of keeping its
    own copy). Consistent week alignment between the regular sync and
    backfill is what lets build_updated_history below de-duplicate
    correctly regardless of which script last touched a given week.
    """
    return d - timedelta(days=d.weekday())


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
    different unit, silently wrong.

    "date" is the Monday of the current week, not date.today() — see
    build_updated_history's docstring for why that distinction matters."""
    gsc = r.get("_gsc_week", {})
    ga4 = r.get("_ga4_week", {})
    return {
        "date": week_start(date.today()).isoformat(),
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
    Returns the new full index, one point per listing per week, each list
    capped at HISTORY_MAX_WEEKS.

    FIXED: this used to always append a new point stamped with
    date.today(), with no check for whether a point already existed for
    the current week. Running the sync more than once in the same week —
    exactly what happened when it was manually re-run to verify a fix —
    appended a second, nearly-identical point instead of replacing the
    first one. Since each point is itself a rolling 7-day window, two
    points close together overlap heavily in which real days they're
    counting, so the chart visually showed what looked like separate
    activity that was substantially the same week counted twice. Now
    replaces any existing point for the same week (by the date key
    make_history_point produces — the Monday of the current week) instead
    of appending, so re-running the sync any number of times in the same
    week is safe and just keeps that week's point fresh, never duplicates it.
    """
    new_index = {}
    for r in results:
        url = r.get("url")
        if not url:
            continue
        prior = list(existing_history_index.get(url, []))
        if "gsc" in r and "ga4" in r:
            point = make_history_point(r)
            prior = [p for p in prior if p.get("date") != point["date"]]
            prior.append(point)
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

    IMPORTANT: GitHub's Contents API only returns file content inline
    (base64 in the `content` field) for files under 1 MiB (1,048,576 bytes).
    Past that, `content` is omitted entirely and the API expects you to use
    the `download_url` it still provides instead. history.json crossed that
    line on 2026-09-28 (1,072,223 bytes) and every history point for every
    listing silently reset to empty that run — not a partial loss, every
    single listing lost its accumulated history at once, because this
    function had no fallback and treated the missing `content` field as
    "nothing to parse, start fresh". Confirmed by matching that exact wipe
    date to the file crossing the exact 1 MiB boundary. Falls back to
    `download_url` now so this can't recur purely from file growth. See
    also HISTORY_MAX_WEEKS above — the file will keep growing indefinitely
    at 52 weeks retention across a growing listing count, so this problem
    class (some future limit, even with the fallback) is worth revisiting
    if the file keeps climbing past a few MB.
    """
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    resp = requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH})
    if resp.status_code != 200:
        return None, None
    data = resp.json()
    sha = data.get("sha")

    raw_content = data.get("content")
    if raw_content:
        try:
            content = base64.b64decode(raw_content).decode("utf-8")
            return json.loads(content), sha
        except Exception as e:
            print(f"  Could not parse existing {file_path} ({e}) — starting fresh.")
            return None, sha

    # content was empty/absent — almost certainly the >1MiB case, not a
    # missing file (a missing file returns 404 above, not 200 with no
    # content). Fetch the raw bytes directly instead.
    download_url = data.get("download_url")
    if not download_url:
        print(f"  {file_path} returned no inline content and no download_url — starting fresh.")
        return None, sha
    try:
        raw_resp = requests.get(download_url)
        raw_resp.raise_for_status()
        return json.loads(raw_resp.text), sha
    except Exception as e:
        print(f"  Could not fetch/parse {file_path} via download_url ({e}) — starting fresh.")
        return None, sha


def fetch_and_publish_reviews():
    """
    Publishes reviews.json — {user_id: [review, ...]} for every APPROVED
    review (review_status == 2) sitewide, grouped by the reviewed
    educator's user_id. Consumed by Widget 55 (LWEA - Listing Reviews) so
    a listing page can show that educator's real reviews without an
    educator needing to paste testimonial text into every listing — the
    exact behavior that caused the duplicate-content problems found and
    fixed on several accounts this session (Deanna, Paige, Amy).

    NOT called from this file's own main() — reviews need to show up far
    faster than this script's weekly cadence allows (a new review
    shouldn't take up to 7 days to appear). Called instead by
    reviews_sync.py, a small standalone script on its own, much more
    frequent cron schedule. Defined here (rather than duplicated there)
    so it shares bd_get/publish_to_github and this file's auth constants
    with everything else — reviews_sync.py just imports this function.

    UNVERIFIED: the endpoint path and param names below are a best-effort
    guess based on this script's other endpoints' naming convention
    (users_portfolio_groups/get, user/get) — reviews was only confirmed
    reachable through the separate admin-tooling API used to build the
    first version of reviews.json by hand, not through this script's own
    BD_API_KEY auth context. Wrapped defensively: if the endpoint or
    response shape is wrong, this fails quietly and the rest of the
    regular sync run (listings, history) proceeds unaffected — reviews.json
    just doesn't get refreshed that run. Check results after first deploy;
    if review counts aren't climbing as real new reviews come in, this
    endpoint guess needs correcting.
    """
    try:
        resp = bd_get("/users_reviews/get", {
            "where[review_status]": 2,
            "limit": 500,
        })
        rows = resp.get("data") or resp.get("message") or []
        if not isinstance(rows, list):
            print("  reviews fetch: unexpected response shape, skipping this run")
            return

        by_user = {}
        for r in rows:
            uid = str(r.get("user_id", ""))
            if not uid:
                continue
            by_user.setdefault(uid, []).append({
                "review_id": r.get("review_id"),
                "review_title": r.get("review_title", ""),
                "review_description": r.get("review_description", ""),
                "review_name": r.get("review_name", ""),
                "review_added": r.get("review_added", ""),
                "rating_overall": r.get("rating_overall", "5"),
            })

        # Newest first. Two reasons: Widget 55 shows the first 5 in file
        # order, so this is what makes it show each educator's NEWEST
        # five; and a stable order means an unchanged set of reviews
        # always produces an identical file, which the skip check below
        # depends on. review_added is a YYYYMMDDHHMMSS string, so plain
        # string comparison sorts it correctly.
        for uid in by_user:
            by_user[uid].sort(key=lambda rv: str(rv.get("review_added", "")), reverse=True)

        # Skip the write when nothing changed. This job can run often, and
        # without this it commits an identical file every time (noise in
        # the repo history, and a possible Pages rebuild per commit).
        # generated_at is deliberately left out of the comparison, since it
        # changes daily even when no review does. If the existing file
        # can't be read for any reason, fall through and publish.
        existing, _ = fetch_existing_json("listing-stats/reviews.json")
        if existing is not None and existing.get("reviews") == by_user:
            print(f"  reviews unchanged ({len(by_user)} educators, {len(rows)} reviews) — skipped write")
            return

        payload = {"generated_at": date.today().isoformat(), "reviews": by_user}
        publish_to_github(payload, "listing-stats/reviews.json")
        print(f"  reviews.json published — {len(by_user)} educators, {len(rows)} reviews")
    except Exception as e:
        print(f"  reviews fetch failed ({e}) — skipping this run, rest of sync unaffected")


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

    # The weekly history pull is pinned to THIS calendar week — Monday
    # through the earliest of (Sunday, however far each API's own lag lets
    # us see). Computed once, outside the loop, so every listing this run
    # gets the exact same window. A trailing "7 days ending near whenever
    # the script runs" window (the old approach) drifts with run timing —
    # two runs a few days apart can both capture the same underlying days,
    # double-counting real activity across two "weekly" points instead of
    # each point representing one real, non-overlapping week. See
    # get_gsc_stats/get_ga4_stats docstrings for the override mechanism.
    this_week_start = week_start(date.today())
    this_week_end = this_week_start + timedelta(days=6)
    gsc_week_end = min(this_week_end, date.today() - timedelta(days=3))
    ga4_week_end = min(this_week_end, date.today() - timedelta(days=1))

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

            # Fixed-calendar-week pull, for history.json ONLY — see
            # make_history_point's docstring for why this can't reuse the
            # 28-day gsc/ga4 above. include_queries=False since history
            # doesn't need top_queries, saving one GSC call per listing.
            gsc_week = get_gsc_stats(
                gsc_service, listing["url"], include_queries=False,
                start_override=this_week_start, end_override=gsc_week_end,
            )
            ga4_week = get_ga4_stats(
                ga4_client, listing["url"],
                start_override=this_week_start, end_override=ga4_week_end,
            )

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
