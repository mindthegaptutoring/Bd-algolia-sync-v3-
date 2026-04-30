#!/usr/bin/env python3
"""
Optimized BD → Algolia sync script
- Retries on 429/5xx with exponential backoff
- Minimizes sleeps while staying rate‑limit safe
- Designed for Render cron (hourly or 2‑hourly)
"""

import os
import re
import json
import time
import math
import requests
import random
from algoliasearch.search_client import SearchClient
from requests.exceptions import HTTPError, RequestException

# ── Config ───────────────────────────────────────────────────────────────────

BD_BASE            = "https://www.learnwitheveryavenue.com"
BD_BASE_URL        = f"{BD_BASE}/api/v2"
BD_API_KEY         = os.environ["BD_API_KEY"]
ALGOLIA_APP_ID     = os.environ["ALGOLIA_APP_ID"]
ALGOLIA_WRITE_KEY  = os.environ["ALGOLIA_WRITE_KEY"]
ALGOLIA_INDEX_NAME = os.environ.get("ALGOLIA_INDEX_NAME", "educators")

BD_HEADERS = {
    "X-Api-Key":    BD_API_KEY,
    "Content-Type": "application/json",
}

LISTING_DATA_ID  = "6"   # Classes & Resources
LISTING_STATUS   = "1"   # published
ACTIVE_USER      = "2"   # active member
MAX_USER_ID      = 300   # probe up to this ID; safe upper bound

MAX_RECORD_BYTES = 9_500
BIO_CHAR_LIMIT   = 500
SNIPPET_CHARS    = 205

# ── Field mappings ────────────────────────────────────────────────────────────

FORMAT_MAP = {
    "1": "1-on-1 Teaching",
    "2": "Tutoring",
    "3": "Self Paced Classes",
    "4": "Online Group Classes",
    "5": "Resources",
    "6": "Coaching & Mentoring",
}

GRADE_MAP = {
    "prek":            "Pre-K",
    "k2":              "K-2",
    "gr_36":           "Gr 3-6",
    "gr_68":           "Gr 6-8",
    "gr_912":          "Gr 9-12",
    "postsecondary":   "Post-Secondary",
    "adult_education": "Adult Education",
}

SCHEDULING_MAP = {
    "flexible_scheduling":            "Flexible scheduling",
    "meets_at_a_set_weekly_time":     "Meets at a set weekly time",
    "meets_multiple_times_per_week":  "Meets multiple times per week",
    "onetime_session":                "One-time session",
    "selfpaced_no_live_meetings":     "Self-paced (no live meetings)",
}

DELIVERY_MAP = {
    "synchronous":  "Live, scheduled sessions",
    "asynchronous": "Self-paced, learn anytime",
    "hybrid":       "Hybrid, mix of both",
}

# ── HTTP helpers with retry/backoff ───────────────────────────────────────────

SESSION = requests.Session()

def bd_request(method: str, endpoint: str, *, params=None, body=None,
               max_retries: int = 5, base_delay: float = 0.5) -> dict:
    url = f"{BD_BASE_URL}{endpoint}"
    params = params or {}
    for attempt in range(max_retries):
        try:
            resp = SESSION.request(
                method=method,
                url=url,
                headers=BD_HEADERS,
                params=params,
                json=body,
                timeout=30,
            )

            if resp.status_code in (429, 500, 502, 503, 504):
                jitter = random.uniform(0.5, 1.5)
                delay = (base_delay * (2 ** attempt)) * jitter
                print(f"  BD {resp.status_code}, retrying in {delay:.1f}s…")
                time.sleep(delay)
                continue

            resp.raise_for_status()
            text = resp.text.strip()
            return resp.json() if text else {}

        except HTTPError as e:
            if e.response.status_code == 400 and "users_portfolio_groups" in endpoint:
                raise
            print(f"  HTTP error on {endpoint}: {e}")
            raise
        except RequestException as e:
            delay = base_delay * (2 ** attempt)
            print(f"  Network error on {endpoint}: {e}, retrying in {delay:.1f}s…")
            time.sleep(delay)
            continue

    raise RuntimeError(f"Failed BD request {method} {endpoint} after {max_retries} attempts")

def bd_get(endpoint: str, params: dict = None) -> dict:
    return bd_request("GET", endpoint, params=params)

def bd_post(endpoint: str, body: dict) -> dict:
    return bd_request("POST", endpoint, body=body)

# ── User discovery ────────────────────────────────────────────────────────────

def get_total_member_count() -> int:
    try:
        data = bd_post("/user/search", {"limit": 1})
        total = int(data.get("total_members") or 0)
        return max(total, 0)
    except Exception as e:
        print(f"  Could not get total member count, falling back to MAX_USER_ID: {e}")
        return MAX_USER_ID

def get_all_active_users(total_members: int) -> list:
    users = []
    consecutive_misses = 0

    for uid in range(1, MAX_USER_ID + 1):
        if len(users) >= total_members and total_members > 0:
            break

        try:
            data = bd_get("/user/get", params={
                "property":       "user_id",
                "property_value": str(uid),
            })
            msg = data.get("message") or []
            user = msg[0] if isinstance(msg, list) and msg else None

            if user:
                consecutive_misses = 0
                name   = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                sub_id = str(user.get("subscription_id", ""))
                is_active = str(user.get("active", "")) == ACTIVE_USER
                if is_active and name and sub_id not in ("4", "7"):
                    users.append(user)
            else:
                consecutive_misses += 1
                if len(users) > 0 and consecutive_misses >= 50:
                    break

        except Exception as e:
            print(f"  user_id={uid} error: {e}")
            consecutive_misses += 1

        time.sleep(0.3)

    return users

# ── Profile photo fetcher with simple cache ───────────────────────────────────

PHOTO_CACHE: dict[str, str] = {}

def get_profile_photo(user_id: str) -> str:
    if user_id in PHOTO_CACHE:
        return PHOTO_CACHE[user_id]

    try:
        data = bd_get("/users_photo/get", params={
            "property":       "user_id",
            "property_value": user_id,
        })
        print(f"  DEBUG photo raw response for {user_id}: {json.dumps(data)[:500]}")
        msg = data.get("message") or []
        if isinstance(msg, list) and msg:
            photo = msg[0]
            full_url = (photo.get("file_main_full_url")
                        or photo.get("file_full_url")
                        or "").strip()
            if full_url:
                PHOTO_CACHE[user_id] = full_url
                return full_url
            filename = (photo.get("file") or photo.get("filename") or "").strip()
            if filename:
                url = f"{BD_BASE}/pictures/profile/{filename}"
                PHOTO_CACHE[user_id] = url
                return url
    except Exception as e:
        print(f"  photo error for user_id={user_id}: {e}")

    PHOTO_CACHE[user_id] = ""
    return ""

# ── Listing fetcher with retry at call level ──────────────────────────────────

def get_user_listings(user_id: str) -> list:
    all_listings = []
    page_cursor = None

    while True:
        params = {
            "property":       "user_id",
            "property_value": user_id,
        }
        if page_cursor:
            params["page"] = page_cursor

        try:
            data = bd_get("/users_portfolio_groups/get", params=params)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                break
            raise

        msg = data.get("message") or []
        if not isinstance(msg, list) or not msg:
            break

        all_listings.extend(msg)

        next_page   = data.get("next_page")
        total_pages = int(data.get("total_pages") or 1)
        current     = int(data.get("current_page") or 1)

        if next_page and current < total_pages:
            page_cursor = next_page
            time.sleep(0.3)
        else:
            break

    return all_listings

# ── Text utilities ────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()

def truncate_utf8(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip()

def enforce_byte_cap(record: dict) -> dict:
    fields = ["description", "bio", "snippet"]
    while len(json.dumps(record).encode("utf-8")) > MAX_RECORD_BYTES:
        shrunk_any = False
        for field in fields:
            val = record.get(field, "")
            if not val:
                continue
            current_bytes = len(val.encode("utf-8"))
            if current_bytes <= 100:
                continue
            new_bytes = max(100, math.floor(current_bytes * 0.7))
            record[field] = truncate_utf8(val, new_bytes)
            shrunk_any = True
            if len(json.dumps(record).encode("utf-8")) <= MAX_RECORD_BYTES:
                break
        if not shrunk_any:
            break
    return record

def resolve_tags(tags_str: str) -> list:
    if not tags_str:
        return []
    return [t.strip() for t in tags_str.split(",") if t.strip()]

# ── Record builders ───────────────────────────────────────────────────────────

def build_educator_record(user: dict) -> dict:
    uid = str(user.get("user_id", ""))
    bio = strip_html(user.get("about_me") or "")[:BIO_CHAR_LIMIT]

    profile_photo = (user.get("profile_photo") or "").strip()
    if profile_photo and not profile_photo.startswith("http"):
        profile_photo = f"{BD_BASE}/{profile_photo.lstrip('/')}"

    record = {
        "objectID":           f"educator_{uid}",
        "type":               "educator",
        "user_id":            uid,
        "name":               f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
        "company":            (user.get("company") or "").strip(),
        "bio":                bio,
        "search_description": (user.get("search_description") or "").strip(),
        "city":               (user.get("city") or "").strip(),
        "state":              (user.get("state_ln") or "").strip(),
        "country":            (user.get("country_ln") or "").strip(),
        "website":            (user.get("website") or "").strip(),
        "instagram":          (user.get("instagram") or "").strip(),
        "profile_url":        f"{BD_BASE}/{user.get('filename', '').lstrip('/')}",
        "profile_photo":      profile_photo,
        "listing_type":       (user.get("listing_type") or "").strip(),
        "active":             user.get("active"),
        "signup_date":        user.get("signup_date", ""),
        "random_rank":        random.randint(1, 1000000),
    }

    lat = user.get("lat")
    lon = user.get("lon")
    if lat and lon:
        try:
            record["_geoloc"] = {"lat": float(lat), "lng": float(lon)}
        except (ValueError, TypeError):
            pass

    return record

def build_listing_record(listing: dict, educator_photo: str = "") -> dict:
    gid         = str(listing.get("group_id") or "")
    title       = (listing.get("group_name") or "").strip()
    description = strip_html(listing.get("group_desc") or "")
    snippet     = description[:SNIPPET_CHARS]
    tags        = resolve_tags(listing.get("post_tags", ""))

    thumbnail = ""
    portfolio = listing.get("users_portfolio")
    if isinstance(portfolio, dict):
        thumbnail = (
            portfolio.get("file_main_full_url")
            or portfolio.get("file_thumbnail_full_url")
            or ""
        )

    city = state = country = ""
    nested_user = listing.get("user")
    if isinstance(nested_user, dict):
        city    = (nested_user.get("city") or "").strip()
        state   = (nested_user.get("state_ln") or "").strip()
        country = (nested_user.get("country_ln") or "").strip()

    delivery_raw = (listing.get("delivery_method") or "").strip().rstrip("_")
    delivery     = DELIVERY_MAP.get(delivery_raw, delivery_raw)

    format_raw = (listing.get("format") or "").strip()
    format_val = FORMAT_MAP.get(format_raw, format_raw)

    grades_raw = resolve_tags(listing.get("grades", ""))
    grades     = [GRADE_MAP.get(g, g) for g in grades_raw]

    scheduling_raw = resolve_tags(listing.get("scheduling", ""))
    scheduling     = [SCHEDULING_MAP.get(s, s) for s in scheduling_raw]

    record = {
        "objectID":         f"listing_{gid}",
        "type":              "listing",
        "group_id":          gid,
        "user_id":           str(listing.get("user_id") or ""),
        "title":             title,
        "description":       description,
        "snippet":           snippet,
        "thumbnail":         thumbnail,
        "tags":              tags,
        "category":          (listing.get("group_category") or "").strip(),
        "listing_url":       f"{BD_BASE}/{listing.get('group_filename', '').lstrip('/')}",
        "post_link":         (listing.get("post_link") or "").strip(),
        "post_location":     (listing.get("post_location") or "").strip(),
        "class_rates":       (listing.get("class_rates") or "").strip(),
        "grades":            grades,
        "delivery_method":   delivery,
        "format":            format_val,
        "duration":          (listing.get("duration") or "").strip(),
        "scheduling":        scheduling,
        "prerequisites":     (listing.get("prerequisites") or "").strip(),
        "cohort_size":       listing.get("cohort_size"),
        "listing_category":  (listing.get("listing_category") or "").strip(),
        "last_updated":      listing.get("revision_timestamp", ""),
        "city":              city,
        "state":             state,
        "country":           country,
        "profile_photo":     educator_photo,
        "random_rank":       random.randint(1, 1000000),
    }

    return enforce_byte_cap(record)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY)
    index  = client.init_index(ALGOLIA_INDEX_NAME)

    print("Getting total member count…")
    total_members = get_total_member_count()
    print(f"{total_members} total members (approx)\n")

    print(f"Probing user IDs 1–{MAX_USER_ID}…")
    users = get_all_active_users(total_members)
    print(f"{len(users)} active educators found\n")

    listing_records = []

    for i, user in enumerate(users, 1):
        uid  = str(user.get("user_id", ""))
        name = f"{user.get('first_name','')} {user.get('last_name','')}".strip()
        print(f"[{i}/{len(users)}] {name} (user_id={uid})")

        try:
            all_listings = get_user_listings(uid)
            published = [
                l for l in all_listings
                if str(l.get("group_status")) == LISTING_STATUS
                and str(l.get("data_id")) == LISTING_DATA_ID
            ]
            if not published:
                print("  no listings")
            else:
                educator_photo = get_profile_photo(uid)
                for listing in published:
                    listing_records.append(build_listing_record(listing, educator_photo))
                print(f"  {len(published)} published listings")
        except Exception as e:
            print(f"  listings error for user_id={uid}: {e}")

        time.sleep(0.3)

    print(f"\n{len(listing_records)} listing records to push")

    print(f"\nReplacing index '{ALGOLIA_INDEX_NAME}' with {len(listing_records)} listings…")
    index.replace_all_objects(listing_records)
    print("Index replaced. Sync complete.")


if __name__ == "__main__":
    main()
