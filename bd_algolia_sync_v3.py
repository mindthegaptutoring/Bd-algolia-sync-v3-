#!/usr/bin/env python3
"""
Optimized BD → Algolia sync script
- Uses robust API pagination instead of fragile ID guessing
- Retries on 429/5xx with exponential backoff
- Minimizes sleeps while staying rate-limit safe
- Designed for Render cron (hourly or 2-hourly)
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

COHORT_SIZE_MAP = {
    "2_to_5_students": "2–5 students",
    "6_to_10_students": "6–10 students",
    "11_students": "11+ students",
}

GRADE_MAP = {
    "prek":            "Pre-K",
    "k2":              "K-2",
    "gr_36":           "Gr 3-5",
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
    "synchronous":              "Live, scheduled sessions",
    "asynchronous":             "Self-paced, learn anytime",
    "synchronous_asynchronous": "Hybrid, mix of both",
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

# ── Image URL helper ──────────────────────────────────────────────────────────

def resolve_image_url(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith("http"):
        return raw
    return f"{BD_BASE}/{raw.lstrip('/')}"

# ── User discovery ────────────────────────────────────────────────────────────

def get_all_active_users() -> list:
    users = []
    limit = 50  
    offset = 0

    print("  Fetching batches from search API...")
    
    while True:
        try:
            data = bd_post("/user/search", {
                "limit": limit,
                "offset": offset
            })
            
            msg = data.get("message") or []
            if not msg or not isinstance(msg, list):
                break  

            for user in msg:
                uid = str(user.get("user_id", ""))
                first = user.get('first_name', '')
                last = user.get('last_name', '')
                name = f"{first} {last}".strip()
                sub_id = str(user.get("subscription_id", ""))
                active_status = str(user.get("active", ""))
                is_active = (active_status == ACTIVE_USER)
                
                # Filter validation check
                if is_active and name and sub_id not in ("4", "7"):
                    users.append(user)
                else:
                    # Debug print to reveal skipped profiles
                    print(f"  ⚠️ Skipped user_id={uid} ({name or 'No Name'}). Reason -> "
                          f"Active: {is_active} (Status value: '{active_status}'), "
                          f"Sub ID: '{sub_id}'")

            if len(msg) < limit:
                break  
                
            offset += limit
            time.sleep(1.0)  

        except Exception as e:
            print(f"  Error fetching user batch at offset {offset}: {e}")
            time.sleep(5.0)
            break 

    return users

# ── Listing fetcher ───────────────────────────────────────────────────────────

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
            time.sleep(0.8)
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
    profile_photo = resolve_image_url(user.get("image_main_file") or "")

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

    cohort_raw  = resolve_tags(listing.get("cohort_size", ""))
    cohort_size = [COHORT_SIZE_MAP.get(c, c) for c in cohort_raw]

    record = {
        "objectID":          f"listing_{gid}",
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
        "cohort_size":       cohort_size,
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

    print("Beginning system scans for active educators…")
    users = get_all_active_users()
    print(f"\nScan complete: {len(users)} active educators passed filters.\n")

    listing_records = []

    for i, user in enumerate(users, 1):
        uid  = str(user.get("user_id", ""))
        name = f"{user.get('first_name','')} {user.get('last_name','')}".strip()
        print(f"[{i}/{len(users)}] Processing: {name} (user_id={uid})")

        try:
            all_listings = get_user_listings(uid)
            published = [
                l for l in all_listings
                if str(l.get("group_status")) == LISTING_STATUS
                and str(l.get("data_id")) == LISTING_DATA_ID
            ]
            if not published:
                print("  no matching published listings")
            else:
                educator_photo = resolve_image_url(user.get("image_main_file") or "")
                for listing in published:
                    listing_records.append(build_listing_record(listing, educator_photo))
                print(f"  Added {len(published)} listings")
        except Exception as e:
            print(f"  listings error for user_id={uid}: {e}")

        time.sleep(3.0)

    print(f"\nTotal records prepared: {len(listing_records)}")
    print(f"Replacing index '{ALGOLIA_INDEX_NAME}' via Algolia atomic replace...")
    index.replace_all_objects(listing_records)
    print("Index sync completely successful.")


if __name__ == "__main__":
    main()
