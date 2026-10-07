"""
LWEA Listing Stats Sync
========================
Version: 1.5.11
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
  1.5.7 — fixed a silent data-loss bug found after the 2026-10-05 weekly run,
          when a dashboard showed an educator's profile but none of her
          listings. A BD lookup that failed (after bd_request's own retries)
          was swallowed: get_all_active_users counted it as "no such user" and
          skipped the educator entirely, and get_bd_listings caught a failed
          listing fetch, printed it, and moved on. The run then published
          whatever it had, overwriting good data with incomplete data, and
          because history.json was rebuilt only from the rows present, every
          dropped listing also lost its whole weekly history. That run lost
          15 rows (10 of them live pages, including a still-active paying
          educator). Now: failed user lookups are retried after cool-downs and
          never counted as misses; listing fetches retry 3 times with
          backoff; any educator still failing has their previous rows carried
          forward (flagged carried_forward) so they still get fresh Search
          Console / GA4 numbers; the run aborts without publishing if BD is
          failing broadly (15 lookups in a row) or if failures leave it more
          than 10% smaller than the last published file; and history for a
          URL missing from a run is kept for 12 weeks instead of deleted.
  1.5.8 — fixed 1.5.4's weekly points, found while comparing against the
          September server-log traffic report (2026-10-06): the Oct 5 point
          was ZERO for all 155 URLs. 1.5.4 pinned each weekly pull to the
          current Monday-Sunday week, but the cron fires on Monday, before
          the new week has any data either API can return (GA4's end is
          yesterday, which is last Sunday, so the window ended before it
          began), so every Monday run appended a zero point to every chart and
          the week that had just ended was never captured. Now each run pulls
          the two most recent COMPLETE weeks and replaces those points: the
          newest, and the one before it so its last Search Console days (3 day
          lag) get filled in. Current-week points are purged on every run,
          which also clears the zero point already written. Because the
          newest complete week is now overwritten with true calendar-week
          numbers, it also replaces the 2026-09-28 point, which had been
          captured by the older trailing-window logic.
  1.5.9 — history is now keyed by a stable id instead of the URL. BD rewrites a
          listing's URL whenever its title changes, which stranded the old
          URL's weekly history and started the renamed listing's chart empty
          (two listings in one week). Each row now carries id = profile_<user_id>
          or listing_<group_id> (the same objectID convention as the Algolia
          index) and history.json is keyed by it. Old URL-keyed files are
          converted automatically on the first run: each URL is matched to its
          row, or followed through BD's redirect when the listing was renamed
          before this change, and series that land on one id are merged by week.
          A rename is also detected going forward (same id, different url than
          the last published results.json); the old URL is kept as an alias for
          35 days and its traffic is summed into both the 28-day numbers and
          the weekly points, so the weeks around a rename are not split in half.
          results.json rows carry id and, while it applies, aliases; the widget
          looks history up by id and falls back to url. history.json also gains
          a readable id -> current url map under "urls". backfill_history.py
          uses the same keys.
  1.5.11 — posted_date for listings now comes from the group's date_updated
          (its creation date) instead of the cover photo's date_added, which
          re-dated listings whenever a thumbnail was replaced.
  1.5.10 — GitHub calls now retry transient failures, and reads fail loudly.
          A backfill that had finished all 155 listings was lost when the
          final publish got a 502 Bad Gateway from api.github.com: publish had
          no retry. Worse, fetch_existing_json treated ANY non-200 reply as
          "the file does not exist, start from scratch", so a 502 at the START
          of a run would have silently begun from an empty history and
          published it, the same wipe as 2026-09-28. Now every GitHub request
          retries 429/5xx/connection errors/timeouts (10, 20, 40, 80 second
          waits); a read that still fails raises and aborts the run instead of
          starting fresh (only a real 404 means "no file yet"); a 409 on write
          re-reads the sha and tries again. fetch_existing_json(strict=False)
          keeps the old forgiving behavior for the reviews unchanged-check,
          where a failed read just means "publish".
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
from urllib.parse import unquote, urljoin

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
    Creation date for a Classes & Resources listing, as YYYY-MM-DD.

    BD's `group_date` is always null, but the group record's `date_updated`
    (YYYYMMDDHHMMSS) is set when the listing is created and is NOT touched by
    later edits (those only move `revision_timestamp`). Verified Oct 2026
    against the admin "Created" column on three listings, and no listing's
    date_updated precedes its owner's signup date.

    Do NOT use the cover photo's `photo_date_added`: replacing a thumbnail
    re-dates it, which made old listings look brand new (v1.5.11 fix).
    Returns None (not a guess) if the field is missing.
    """
    raw = listing.get("date_updated") or listing.get("group_date") or listing.get("date_created")
    raw = str(raw or "")
    if len(raw) < 8 or not raw[:8].isdigit():
        return None
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def _fetch_user(uid):
    """One /user/get lookup. Returns the user dict, or None if BD says there
    is no such user. RAISES if the request itself failed -- callers must
    treat a failed lookup differently from "no such user"."""
    data = bd_get("/user/get", params={"property": "user_id", "property_value": str(uid)})
    msg = data.get("message") or []
    return msg[0] if isinstance(msg, list) and msg else None


def _user_is_wanted(user):
    sub_id = str(user.get("subscription_id", ""))
    is_active = str(user.get("active", "")) == ACTIVE_USER
    name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
    return bool(is_active and name and sub_id not in ("4", "7"))


def get_all_active_users(failed_uids=None) -> list:
    """Probe user_id 1..MAX_USER_ID, same consecutive-miss stop logic as
    bd_algolia_sync_v3.py (BD has no bulk user list, so this is the only way in).

    FIXED in 1.5.7: a lookup that FAILED (after bd_request's own retries) used
    to be counted as a "miss" and the educator was silently skipped, so one bad
    API moment deleted a real, active educator's profile and every listing
    from the published file. Now a failed lookup is remembered, retried after a
    cool-down (BD rate-limits at ~100 req/min, and other jobs share that
    budget), and anything still failing is reported through failed_uids so the
    caller can carry that educator's previous rows forward. A failed lookup also
    no longer counts toward the 20-consecutive-misses stop. If BD is failing
    on 15 lookups in a row the whole run aborts instead of publishing."""
    users = []
    failed = []
    consecutive_misses = 0
    consecutive_errors = 0
    for uid in range(1, MAX_USER_ID + 1):
        try:
            user = _fetch_user(uid)
            consecutive_errors = 0
        except Exception as e:
            print(f"  user_id={uid} lookup FAILED: {e}")
            failed.append(uid)
            consecutive_errors += 1
            if consecutive_errors >= 15:
                raise RuntimeError(
                    "BD API failed 15 user lookups in a row; aborting the run "
                    "before anything is published so last week's data stays intact."
                )
            time.sleep(1.0)
            continue
        if user:
            consecutive_misses = 0
            if _user_is_wanted(user):
                users.append(user)
            time.sleep(0.5)
        else:
            consecutive_misses += 1
            if users and consecutive_misses >= 20:
                print(f"  20 consecutive misses after user_id={uid - 1}, stopping probe.")
                break

    for wait in (30, 60):
        if not failed:
            break
        print(f"  retrying {len(failed)} failed user lookup(s) after a {wait}s cool-down...")
        time.sleep(wait)
        still_failed = []
        for uid in failed:
            try:
                user = _fetch_user(uid)
            except Exception as e:
                print(f"  user_id={uid} still failing: {e}")
                still_failed.append(uid)
                time.sleep(1.0)
                continue
            if user and _user_is_wanted(user):
                users.append(user)
            time.sleep(0.5)
        failed = still_failed

    users.sort(key=lambda u: int(u.get("user_id") or 0))
    if failed_uids is not None:
        failed_uids.extend(failed)
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


def get_bd_listings(failures=None) -> list:
    """
    Returns [{member_id, member_name, url, title, post_type}, ...] covering
    both each educator's profile page and their individual published
    Classes & Resources listings.

    failures (optional dict) is filled with {"users": [...], "listings": [...]}:
    user_ids whose lookup failed, and user_ids whose listing fetch failed
    even after retries. See main() / carry_forward_failed() for what happens
    to them -- they are NOT treated as "this educator has no listings."
    """
    if failures is None:
        failures = {}
    failures.setdefault("users", [])
    failures.setdefault("listings", [])

    print("Probing BD for active educators...")
    users = get_all_active_users(failed_uids=failures["users"])
    print(f"{len(users)} active educators found")

    results = []
    for i, user in enumerate(users, 1):
        uid = str(user.get("user_id", ""))
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        filename = (user.get("filename") or "").lstrip("/")

        if filename:
            results.append({
                "id": profile_id(uid),
                "member_id": uid,
                "member_name": name,
                "url": f"{BD_BASE}/{filename}",
                "title": name,
                "post_type": "profile",
                "posted_date": (user.get("signup_date") or "")[:10] or None,  # BD returns ISO datetime, take just the date part
            })

        print(f"  [{i}/{len(users)}] {name} (user_id={uid}) - fetching listings")
        user_listings = None
        for attempt in range(3):
            try:
                user_listings = get_user_listings(uid)
                break
            except Exception as e:
                print(f"  listings error for user_id={uid} (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(20 * (attempt + 1))
        if user_listings is None:
            failures["listings"].append(uid)
        else:
            for listing in user_listings:
                group_filename = (listing.get("group_filename") or "").lstrip("/")
                if not group_filename:
                    continue
                gid = listing.get("group_id")
                results.append({
                    "id": listing_id(gid) if gid else None,
                    "member_id": uid,
                    "member_name": name,
                    "url": f"{BD_BASE}/{group_filename}",
                    "title": (listing.get("group_name") or "").strip(),
                    "post_type": "listing",
                    "posted_date": _extract_posted_date_iso(listing),
                })

        time.sleep(3.0)  # same per-user pacing as bd_algolia_sync_v3.py -- keeps this under BD's ~100 req/min limit

    return results


def carry_forward_failed(listings, failures, prev_rows=None):
    """
    For educators whose BD lookup or listing fetch FAILED this run (not
    educators BD successfully reported as inactive or having no listings),
    re-add their rows from the last published results.json so they still get
    fresh Search Console / GA4 numbers instead of vanishing. Their title,
    URL and posted date are reused; only the BD lookup is skipped.

    Also the publish guard: if there were failures and the run still ends up
    more than 10% smaller than the last published file, abort instead of
    overwriting good data with a lopsided run.
    """
    failed_ids = {str(u) for u in failures.get("users", [])} | {str(u) for u in failures.get("listings", [])}
    if not failed_ids:
        return listings

    print(f"BD lookups failed for user_id(s): {sorted(failed_ids, key=int)}")
    if prev_rows is None:
        prev, _ = fetch_existing_json(GITHUB_FILE_PATH)
        prev_rows = (prev or {}).get("listings") or []
    have = {l["url"] for l in listings}
    carried = []
    for row in prev_rows:
        if str(row.get("member_id")) in failed_ids and row.get("url") not in have:
            carried.append({
                # rows published before 1.5.9 have no id; a profile's is derivable
                "id": row.get("id") or (profile_id(row.get("member_id")) if row.get("post_type") == "profile" else None),
                "aliases": row.get("aliases"),
                "member_id": row.get("member_id"),
                "member_name": row.get("member_name"),
                "url": row.get("url"),
                "title": row.get("title"),
                "post_type": row.get("post_type"),
                "posted_date": row.get("posted_date"),
                "carried_forward": True,
            })
    print(f"Carrying forward {len(carried)} row(s) from the last published results for those educators.")
    merged = listings + carried

    if prev_rows and len(merged) < 0.9 * len(prev_rows):
        raise SystemExit(
            f"ABORTING: BD lookups failed AND this run has {len(merged)} rows vs "
            f"{len(prev_rows)} last time (more than 10% smaller). Nothing was published, "
            f"so the current results.json and history.json are untouched. Re-run once BD is responding."
        )
    return merged


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


# ---------------------------------------------------------------------------
# 6a. Stable IDs, renames, and history keyed by ID (NEW in 1.5.9)
#
# History used to be keyed by URL. BD regenerates a listing's URL whenever its
# title changes, so a rename left the old URL's weekly history stranded and the
# new URL started with an empty chart (seen when two listings were renamed in
# one week). Every row now carries an "id" that never changes -- profile_<user_id>
# or listing_<group_id>, the same objectID convention bd_algolia_sync_v3.py uses
# in Algolia -- and history.json is keyed by that.
# ---------------------------------------------------------------------------
ALIAS_DAYS = 35   # how long a renamed listing's OLD url keeps being queried alongside the new one


def profile_id(user_id):
    return f"profile_{user_id}"


def listing_id(group_id):
    return f"listing_{group_id}"


def row_key(r):
    """History key for a result row: its stable id, or (only if BD gave no
    group_id) a url-based fallback so the row still gets a series."""
    return r.get("id") or ("url:" + str(r.get("url", "")))


def _norm_url(u):
    return unquote(str(u)).rstrip("/").lower()


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def merge_period_stats(a, b):
    """
    Combine two stats dicts for the SAME period from two URLs (a renamed
    listing's new and old address). Counts and dict-of-counts are summed;
    ctr, position and avg_engagement_seconds are recomputed from the sums
    (position weighted by impressions, engagement by sessions); lists such as
    top_queries are taken from `a`, the primary. Works on both the 28-day
    snapshot dicts and the weekly history points.
    """
    out = dict(a)
    for k, v in b.items():
        if k in ("ctr", "position", "avg_engagement_seconds", "top_queries"):
            continue
        if _num(v):
            out[k] = (a.get(k) if _num(a.get(k)) else 0) + v
        elif isinstance(v, dict) and v and all(_num(x) for x in v.values()):
            d = dict(a.get(k) or {})
            for kk, vv in v.items():
                d[kk] = (d.get(kk) or 0) + vv
            out[k] = d
    ia, ib = (a.get("impressions") or 0), (b.get("impressions") or 0)
    sa, sb = (a.get("sessions") or 0), (b.get("sessions") or 0)
    if "position" in a or "position" in b:
        pa, pb = (a.get("position") or 0.0), (b.get("position") or 0.0)
        out["position"] = round((pa * ia + pb * ib) / (ia + ib), 1) if (ia + ib) else (pa or pb or 0.0)
    if "ctr" in a or "ctr" in b:
        imp = out.get("impressions") or 0
        out["ctr"] = ((out.get("clicks") or 0) / imp) if imp else 0.0
    if "avg_engagement_seconds" in a or "avg_engagement_seconds" in b:
        ea, eb = (a.get("avg_engagement_seconds") or 0.0), (b.get("avg_engagement_seconds") or 0.0)
        out["avg_engagement_seconds"] = round((ea * sa + eb * sb) / (sa + sb), 1) if (sa + sb) else (ea or eb or 0.0)
    return out


def fetch_period(fn, urls, **kw):
    """Run a per-URL stats function for the current URL and any aliases, and
    merge. The first URL is the primary: if it fails the error propagates (the
    row becomes an error row, as before); a failing alias is skipped, since it
    only adds a rename's pre-rename traffic."""
    out = fn(urls[0], **kw)
    for u in urls[1:]:
        try:
            out = merge_period_stats(out, fn(u, **kw))
        except Exception as e:
            print(f"  alias url failed, skipping ({u}): {e}")
    return out


def merge_history_point(a, b):
    """Two weekly points for the same week (the same listing under two URLs)."""
    out = merge_period_stats(a, b)
    out["date"] = a.get("date") or b.get("date")
    out["triage_flag"] = b.get("triage_flag") if b.get("triage_flag") is not None else a.get("triage_flag")
    return out


def merge_history_lists(a, b):
    """Merge two weekly-point lists by week; an overlapping week is summed."""
    by_date = {p["date"]: p for p in a}
    for p in b:
        by_date[p["date"]] = merge_history_point(by_date[p["date"]], p) if p["date"] in by_date else p
    return [by_date[d] for d in sorted(by_date)]


def resolve_redirect(url, max_hops=3):
    """Follow a URL's redirects (BD 301s an old slug to the new one) and return
    where it lands, or None. Only used once, to migrate old URL-keyed history."""
    cur = url
    for _ in range(max_hops):
        try:
            r = requests.get(cur, allow_redirects=False, timeout=20, headers={"User-Agent": "LWEA-stats-sync"})
        except Exception:
            return None
        loc = r.headers.get("Location")
        if r.status_code in (301, 302, 303, 307, 308) and loc:
            try:   # requests decodes header bytes as latin-1; slugs with emoji are raw UTF-8
                loc = loc.encode("latin-1").decode("utf-8")
            except Exception:
                pass
            cur = urljoin(cur, loc)
        else:
            return cur if cur != url else None
    return cur


def _active_weeks(points):
    return {p["date"] for p in points if (p.get("sessions") or 0) + (p.get("impressions") or 0) > 0}


def migrate_history_keys(history_index, rows, resolve=None):
    """
    One-time, automatic: convert URL-keyed history (keys starting "http") to
    id-keyed.

    1. A URL equal to a row's current URL or a known alias is that row's series.
    2. Any other URL (a listing renamed before aliases existed, or a page that
       was deleted) is followed through BD's redirect. If it lands on a row's
       URL it is merged into that row's series, by week, but ONLY when the two
       series never ran side by side (at most one week of activity in common).
       A rename is sequential: the old URL's traffic stops as the new one
       starts. A deleted duplicate that redirects to a surviving listing has
       traffic running in parallel with it, and merging that would inflate the
       survivor's chart, so it is kept apart instead.
    3. Anything left over is kept under "url:<url>" and ages out after 12 weeks
       like any other absent series.
    No-op once the keys are ids.
    """
    legacy = [k for k in history_index if str(k).startswith("http")]
    if not legacy:
        return history_index
    url_to_id = {}
    for r in rows:
        rid = r.get("id")
        if not rid:
            continue
        url_to_id[_norm_url(r["url"])] = rid
        for al in r.get("aliases") or []:
            url_to_id.setdefault(_norm_url(al["url"]), rid)
    resolver = resolve if resolve is not None else resolve_redirect
    out = {k: v for k, v in history_index.items() if k not in legacy}
    direct = followed = parallel = orphaned = 0

    pending = []
    for url in legacy:                       # pass 1: exact matches, so every target series exists
        rid = url_to_id.get(_norm_url(url))
        if rid is None:
            pending.append(url)
            continue
        out[rid] = merge_history_lists(out[rid], history_index[url]) if rid in out else list(history_index[url])
        direct += 1

    for url in pending:                      # pass 2: renamed or deleted pages
        pts = history_index[url]
        final = resolver(url)
        rid = url_to_id.get(_norm_url(final)) if final else None
        if rid is not None and len(_active_weeks(pts) & _active_weeks(out.get(rid, []))) <= 1:
            out[rid] = merge_history_lists(out[rid], pts) if rid in out else list(pts)
            followed += 1
            continue
        if rid is not None:
            parallel += 1
        else:
            orphaned += 1
        out["url:" + url] = list(pts)
    print(f"  history keys migrated to ids: {direct} direct, {followed} renamed (merged via redirect), "
          f"{parallel} redirect to a listing with parallel traffic (kept apart), {orphaned} unmatched (kept as url: keys)")
    return out


def attach_aliases(listings, prev_rows, today=None):
    """
    Detect renames: a row whose id is in the last published results.json under a
    DIFFERENT url was renamed. Record the old url as an alias (with the date it
    was first seen) so its pre-rename traffic keeps being added in for ALIAS_DAYS,
    long enough for the weeks around the rename to settle. Aliases already on
    the previous row are carried forward until they expire.
    """
    today = today or date.today()
    prev_by_id = {r["id"]: r for r in (prev_rows or []) if r.get("id")}
    cutoff = (today - timedelta(days=ALIAS_DAYS)).isoformat()
    renamed = 0
    for row in listings:
        prev = prev_by_id.get(row.get("id")) if row.get("id") else None
        if not prev:
            continue
        aliases = {al["url"]: al["since"] for al in (prev.get("aliases") or [])}
        if prev.get("url") and prev["url"] != row["url"]:
            if prev["url"] not in aliases:
                renamed += 1
            aliases.setdefault(prev["url"], today.isoformat())
        aliases.pop(row["url"], None)
        aliases = {u: since for u, since in aliases.items() if since >= cutoff}
        if aliases:
            row["aliases"] = [{"url": u, "since": since} for u, since in aliases.items()]
    if renamed:
        print(f"  {renamed} renamed listing(s) detected; their old URLs will be queried alongside the new ones for {ALIAS_DAYS} days")
    return listings


def history_week_windows(today=None):
    """
    The two most recent COMPLETE Monday-Sunday weeks, oldest first, as
    (week_start, gsc_end, ga4_end) tuples. Each end is capped at what that
    API can actually see yet (Search Console lags about 3 days, GA4 about 1).

    Two weeks, not one, because the newest complete week is still missing its
    last couple of Search Console days when the Monday cron runs; the next
    run's second window re-pulls that week once the data has settled, and
    build_updated_history replaces the earlier, partial numbers.

    NEVER the current, unfinished week. 1.5.4 pinned the weekly pull to the
    current week, but the cron runs Monday, before the new week has any data
    either API can return, so every Monday run appended a point of all zeros
    to every chart (found 2026-10-06, the week 1.5.4 first ran live).
    """
    today = today or date.today()
    last_ws = week_start(today) - timedelta(days=7)
    out = []
    for ws in (last_ws - timedelta(days=7), last_ws):
        we = ws + timedelta(days=6)
        out.append((ws, min(we, today - timedelta(days=3)), min(we, today - timedelta(days=1))))
    return out


def make_history_points(r):
    """
    Compact weekly snapshots for the trend chart, one per week in r["_weeks"]
    (see history_week_windows). Deliberately lean: only what a trend line
    needs, no top_queries and no traffic_sources breakdown.

    IMPORTANT: reads the fixed-week pulls in r["_weeks"], NOT r["gsc"] /
    r["ga4"], which are 28-day rolling totals. Mixing a 28-day total into the
    same array as true 7-day weekly points would make every point look about
    4x too high, same number in a different unit.

    triage_flag is a snapshot of THIS run's judgment, so only the newest week
    gets it; the older, re-pulled week gets None and build_updated_history
    keeps whatever flag that point already had.
    """
    points = []
    weeks = r.get("_weeks") or []
    for i, wk in enumerate(weeks):
        gsc = wk.get("gsc", {})
        ga4 = wk.get("ga4", {})
        newest = (i == len(weeks) - 1)
        points.append({
            "date": wk["week_start"],
            "impressions": gsc.get("impressions", 0),
            "clicks": gsc.get("clicks", 0),
            "ctr": gsc.get("ctr", 0.0),
            "position": gsc.get("position", 0.0),
            "sessions": ga4.get("sessions", 0),
            "avg_engagement_seconds": ga4.get("avg_engagement_seconds", 0.0),
            "contact_clicks": ga4.get("contact_clicks"),
            "connect_pageviews": ga4.get("connect_pageviews"),
            "triage_flag": r.get("triage_flag", "error") if newest else None,
        })
    return points


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
    # Convert any URL-keyed history from before 1.5.9 (a no-op once converted).
    existing_history_index = migrate_history_keys(existing_history_index, results)
    new_index = {}
    this_monday = week_start(date.today()).isoformat()
    for r in results:
        if not r.get("url"):
            continue
        key = row_key(r)
        # Drop any point for the current (unfinished) week or later. Only
        # complete weeks are valid history, and this also clears the all-zero
        # point that 1.5.4 wrote on its first Monday run.
        prior = [p for p in existing_history_index.get(key, []) if str(p.get("date", "")) < this_monday]
        if "gsc" in r and "ga4" in r:
            for point in make_history_points(r):
                existing = next((p for p in prior if p.get("date") == point["date"]), None)
                if existing is not None and point.get("triage_flag") is None:
                    point["triage_flag"] = existing.get("triage_flag")   # keep the older week's own flag
                prior = [p for p in prior if p.get("date") != point["date"]]
                prior.append(point)
            prior.sort(key=lambda p: str(p.get("date", "")))
        new_index[key] = prior[-HISTORY_MAX_WEEKS:]

    # Keep the history of any URL that is missing from THIS run, as long as it
    # has a point from the last 12 weeks. Before 1.5.7 this index was built only
    # from the rows present in the run, so a listing that dropped out for one
    # run (a failed BD fetch) lost its whole weekly history the same moment.
    # Pruning after 12 weeks stops deleted listings from piling up forever.
    cutoff = (date.today() - timedelta(days=84)).isoformat()
    for key, pts in existing_history_index.items():
        if key not in new_index and pts and str(pts[-1].get("date", "")) >= cutoff:
            new_index[key] = [p for p in pts if str(p.get("date", "")) < this_monday]
    return new_index



# ---------------------------------------------------------------------------
# 7. GitHub read/publish — generalized in 1.4.0 to take a file_path, since
#    two files are now published each run instead of one
# ---------------------------------------------------------------------------
GITHUB_RETRY_STATUSES = (429, 500, 502, 503, 504)


def _github_call(fn, what, attempts=5):
    """
    Run a GitHub request (a zero-argument function returning a Response),
    retrying transient failures: 429 and 5xx replies, connection errors and
    timeouts. Waits 10, 20, 40, 80 seconds between attempts. A non-retryable
    reply (200, 404, 409, 422...) is returned for the caller to judge. If every
    attempt fails, the last HTTP error is raised, or RuntimeError if there was
    never a reply. Added in 1.5.10 after a 502 from GitHub's API on the final
    publish discarded a ~20 minute backfill that had otherwise finished.
    """
    last = None
    for attempt in range(1, attempts + 1):
        resp = None
        try:
            resp = fn()
            if resp.status_code not in GITHUB_RETRY_STATUSES:
                return resp
            last = f"HTTP {resp.status_code}"
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last = type(e).__name__
        if attempt < attempts:
            wait = min(120, 10 * 2 ** (attempt - 1))
            print(f"  GitHub {what} failed ({last}); retry {attempt}/{attempts - 1} in {wait}s")
            time.sleep(wait)
    if resp is not None:
        resp.raise_for_status()
    raise RuntimeError(f"GitHub {what} failed after {attempts} attempts ({last})")


def fetch_existing_json(file_path, strict=True):
    """
    GET the currently-published JSON at file_path (content + sha), so a run
    can build on top of it instead of starting fresh. Returns
    (parsed_json_or_None, sha_or_None).

    A file that genuinely does not exist (404) returns (None, None), and
    callers treat that as "start from scratch". Anything else that goes wrong
    reading it (a 5xx that survives retries, a network failure, content that
    cannot be parsed) RAISES when strict=True, the default, so a run aborts
    instead of quietly starting from an empty file and publishing that over the
    real history. That used to be the behavior here: any non-200 reply meant
    "start fresh", so one transient GitHub error at the start of a run would
    have wiped history exactly as the 1 MiB problem below did. strict=False
    keeps the old forgiving behavior for callers where that is harmless.

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

    def fail(msg, exc=None):
        if strict:
            raise RuntimeError(f"{msg} Aborting instead of starting from an empty file.") from exc
        print(f"  {msg} Starting fresh.")
        return None, None

    try:
        resp = _github_call(
            lambda: requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH}, timeout=60),
            f"read of {file_path}")
    except Exception as e:
        return fail(f"Could not read {file_path} from GitHub ({e}).", e)
    if resp.status_code == 404:
        return None, None
    if resp.status_code != 200:
        return fail(f"Could not read {file_path} from GitHub (HTTP {resp.status_code}).")
    data = resp.json()
    sha = data.get("sha")

    raw_content = data.get("content")
    if raw_content:
        try:
            return json.loads(base64.b64decode(raw_content).decode("utf-8")), sha
        except Exception as e:
            return fail(f"Could not parse existing {file_path} ({e}).", e)

    # content was empty/absent: almost certainly the >1MiB case, not a missing
    # file (a missing file returns 404 above). Fetch the raw bytes instead.
    download_url = data.get("download_url")
    if not download_url:
        return fail(f"{file_path} returned no inline content and no download_url.")
    try:
        raw_resp = _github_call(lambda: requests.get(download_url, timeout=120), f"download of {file_path}")
        raw_resp.raise_for_status()
        return json.loads(raw_resp.text), sha
    except Exception as e:
        return fail(f"Could not fetch/parse {file_path} via download_url ({e}).", e)


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
        existing, _ = fetch_existing_json("listing-stats/reviews.json", strict=False)   # a failed read just means "publish"
        if existing is not None and existing.get("reviews") == by_user:
            print(f"  reviews unchanged ({len(by_user)} educators, {len(rows)} reviews) — skipped write")
            return

        payload = {"generated_at": date.today().isoformat(), "reviews": by_user}
        publish_to_github(payload, "listing-stats/reviews.json")
        print(f"  reviews.json published — {len(by_user)} educators, {len(rows)} reviews")
    except Exception as e:
        print(f"  reviews fetch failed ({e}) — skipping this run, rest of sync unaffected")


def publish_to_github(payload, file_path):
    """
    Write payload to file_path in the repo through the Contents API. Every
    request retries through transient GitHub errors (see _github_call). The
    sha is read fresh right before each write; a 409 (the file changed in the
    seconds between the read and the write, or an earlier attempt that returned
    a 502 had actually landed) re-reads the sha and writes again, up to 3 times.
    """
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    content_b64 = base64.b64encode(json.dumps(payload, indent=2).encode()).decode()

    for attempt in range(1, 4):
        existing = _github_call(
            lambda: requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH}, timeout=60),
            f"sha lookup for {file_path}")
        sha = existing.json().get("sha") if existing.status_code == 200 else None
        body = {
            "message": f"Update {file_path} — {date.today().isoformat()}",
            "content": content_b64,
            "branch": GITHUB_BRANCH,
        }
        if sha:
            body["sha"] = sha
        put_resp = _github_call(
            lambda: requests.put(api_url, headers=headers, json=body, timeout=180),
            f"write of {file_path}")
        if put_resp.status_code == 409 and attempt < 3:
            print(f"  {file_path} changed while writing (409); re-reading and trying again ({attempt}/2)")
            continue
        put_resp.raise_for_status()
        return


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    creds = get_google_credentials()
    gsc_service = gbuild("searchconsole", "v1", credentials=creds)
    ga4_client = BetaAnalyticsDataClient(credentials=creds)

    bd_failures = {}
    listings = get_bd_listings(bd_failures)
    prev_results, _ = fetch_existing_json(GITHUB_FILE_PATH)
    prev_rows = (prev_results or {}).get("listings") or []
    listings = carry_forward_failed(listings, bd_failures, prev_rows)
    listings = attach_aliases(listings, prev_rows)
    results = []

    # Weekly history pulls: the two most recent COMPLETE Monday-Sunday weeks,
    # computed once so every listing gets identical windows. See
    # history_week_windows for why it is two weeks and never the current one.
    week_windows = history_week_windows()

    # Pass 1: collect every listing's raw GSC/GA4 data. No triage flag yet --
    # the percentile cutoffs below can only be computed once this run's full
    # distribution is known.
    for listing in listings:
        try:
            # A renamed listing's old URL(s) are queried too and summed in, so a
            # rename never drops its earlier traffic out of the numbers.
            urls = [listing["url"]] + [a["url"] for a in listing.get("aliases", [])]
            gsc = fetch_period(lambda u, **kw: get_gsc_stats(gsc_service, u, **kw), urls)
            ga4 = fetch_period(lambda u, **kw: get_ga4_stats(ga4_client, u, **kw), urls)
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

            # Fixed complete-week pulls, for history.json ONLY: not the 28-day
            # gsc/ga4 above (different unit, see make_history_points).
            # include_queries=False since history doesn't need top_queries.
            weeks = []
            for ws, gsc_end, ga4_end in week_windows:
                weeks.append({
                    "week_start": ws.isoformat(),
                    "gsc": fetch_period(lambda u, **kw: get_gsc_stats(gsc_service, u, include_queries=False, **kw),
                                        urls, start_override=ws, end_override=gsc_end),
                    "ga4": fetch_period(lambda u, **kw: get_ga4_stats(ga4_client, u, **kw),
                                        urls, start_override=ws, end_override=ga4_end),
                })

            results.append({**listing, "gsc": gsc, "ga4": ga4, "_weeks": weeks})
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
        r.pop("_weeks", None)
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
        "history": new_history_index,  # {id: [weekly_point, ...]}; id is profile_<user_id> or listing_<group_id>
        "urls": {row_key(r): r["url"] for r in results if r.get("url")},   # id -> current url, for humans reading the file
    }
    publish_to_github(history_payload, GITHUB_HISTORY_FILE_PATH)

    print(f"Synced {len(results)} listings. History now covers up to {HISTORY_MAX_WEEKS} weeks per listing.")


if __name__ == "__main__":
    main()
