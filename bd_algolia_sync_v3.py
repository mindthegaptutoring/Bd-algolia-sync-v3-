#!/usr/bin/env python3
"""
bd_algolia_sync.py  v3.9
Syncs educators and listings to Algolia via BD API v2.

Based on official BD API documentation:
  - POST /api/v2/user/search with output_type=array returns JSON user array
  - GET /api/v2/user/get?property=user_id&property_value={id} returns full user data
  - GET /api/v2/users_portfolio_groups/get?property=user_id&property_value={id}
    returns all listings for a user

Flow:
  1. POST user/search (output_type=array) to get all user_ids
  2. GET user/get for each user_id to get full profile data (about_me, photos, etc)
  3. GET users_portfolio_groups/get for each user_id to get their listings
  4. Filter listings: group_status=1, data_id=6
  5. Push all records to Algolia

GitHub Actions secrets required:
  BD_API_KEY
  ALGOLIA_APP_ID
  ALGOLIA_WRITE_KEY
  ALGOLIA_INDEX_NAME  (optional, defaults to "educators")
"""

import os
import re
import json
import time
import requests
from algoliasearch.search_client import SearchClient

# ── Config ───────────────────────────────────────────────────────────────────

BD_BASE            = "https://learn.everyavenue.com"
BD_BASE_URL        = f"{BD_BASE}/api/v2"
BD_API_KEY         = os.environ["BD_API_KEY"]
ALGOLIA_APP_ID     = os.environ["ALGOLIA_APP_ID"]
ALGOLIA_WRITE_KEY  = os.environ["ALGOLIA_WRITE_KEY"]
ALGOLIA_INDEX_NAME = os.environ.get("ALGOLIA_INDEX_NAME", "educators")

BD_HEADERS = {
    "X-Api-Key":    BD_API_KEY,
    "Content-Type": "application/json",
}

LISTING_DATA_ID = "6"   # Classes & Resources
LISTING_STATUS  = "1"   # published
ACTIVE_USER     = "2"   # active member

MAX_RECORD_BYTES = 9_500
BIO_CHAR_LIMIT   = 500
SNIPPET_CHARS    = 205


# ── BD API helpers ────────────────────────────────────────────────────────────

def bd_get(endpoint: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{BD_BASE_URL}{endpoint}",
        headers=BD_HEADERS,
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    if not resp.text.strip():
        return {}
    return resp.json()


def bd_post(endpoint: str, body: dict) -> dict:
    resp = requests.post(
        f"{BD_BASE_URL}{endpoint}",
        headers=BD_HEADERS,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    if not resp.text.strip():
        return {}
    return resp.json()


def get_all_user_ids() -> list:
    """
    POST /api/v2/user/search with output_type=array to retrieve all user_ids.
    Paginates using page parameter and total_pages from response.
    Returns list of user_id strings for active members only.
    """
    user_ids = []
    page = 1

    while True:
        data = bd_post("/user/search", {
            "output_type": "array",
            "q":           "",
            "limit":       100,
            "page":        page,
        })

        members = data.get("message") or []
        if not isinstance(members, list) or not members:
            print(f"  page {page}: no members returned")
            break

        active = [m for m in members if str(m.get("active", "")) == ACTIVE_USER]
        user_ids.extend([str(m["user_id"]) for m in active if m.get("user_id")])
        print(f"  page {page}: {len(members)} members, {len(active)} active")

        total_pages = int(data.get("total_pages") or 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    return user_ids


def get_user(user_id: str) -> dict:
    """GET full user profile including about_me, search_description, photos."""
    data = bd_get("/user/get", params={
        "property":       "user_id",
        "property_value": user_id,
    })
    msg = data.get("message") or []
    return msg[0] if isinstance(msg, list) and msg else {}


def get_user_listings(user_id: str) -> list:
    """
    GET all portfolio groups (listings) for a user.
    Paginates using next_page cursor from response.
    """
    listings = []
    page = 1

    while True:
        data = bd_get("/users_portfolio_groups/get", params={
            "property":       "user_id",
            "property_value": user_id,
            "page":           page,
            "limit":          100,
        })

        msg = data.get("message") or []
        if not isinstance(msg, list) or not msg:
            break

        listings.extend(msg)

        total_pages = int(data.get("total_pages") or 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    return listings


# ── Text utilities ────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def truncate_utf8(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip()


def enforce_byte_cap(record: dict) -> dict:
    for field in ("description", "bio", "snippet"):
        while len(json.dumps(record).encode("utf-8")) > MAX_RECORD_BYTES:
            val = record.get(field, "")
            if not val:
                break
            record[field] = truncate_utf8(val, len(val.encode("utf-8")) // 2)
    return record


def resolve_tags(tags_str: str) -> list:
    if not tags_str:
        return []
    return [t.strip() for t in tags_str.split(",") if t.strip()]


# ── Record builders ───────────────────────────────────────────────────────────

def build_educator_record(user: dict) -> dict:
    uid = str(user.get("user_id", ""))
    bio = strip_html(user.get("about_me") or "")[:BIO_CHAR_LIMIT]

    # Profile photo — may be a relative path or full URL
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
    }

    lat = user.get("lat")
    lon = user.get("lon")
    if lat and lon:
        try:
            record["_geoloc"] = {"lat": float(lat), "lng": float(lon)}
        except (ValueError, TypeError):
            pass

    return record


def build_listing_record(listing: dict) -> dict:
    gid         = str(listing.get("group_id") or "")
    title       = (listing.get("group_name") or "").strip()
    description = strip_html(listing.get("group_desc") or "")
    snippet     = description[:SNIPPET_CHARS]
    tags        = resolve_tags(listing.get("post_tags", ""))

    # Thumbnail from nested users_portfolio
    thumbnail = ""
    portfolio = listing.get("users_portfolio")
    if isinstance(portfolio, dict):
        thumbnail = (
            portfolio.get("file_main_full_url")
            or portfolio.get("file_thumbnail_full_url")
            or ""
        )

    # Educator location from nested user object
    city = state = country = ""
    nested_user = listing.get("user")
    if isinstance(nested_user, dict):
        city    = (nested_user.get("city") or "").strip()
        state   = (nested_user.get("state_ln") or "").strip()
        country = (nested_user.get("country_ln") or "").strip()

    record = {
        "objectID":         f"listing_{gid}",
        "type":             "listing",
        "group_id":         gid,
        "user_id":          str(listing.get("user_id") or ""),
        "title":            title,
        "description":      description,
        "snippet":          snippet,
        "thumbnail":        thumbnail,
        "tags":             tags,
        "group_category":   (listing.get("group_category") or "").strip(),
        "listing_url":      f"{BD_BASE}/{listing.get('group_filename', '').lstrip('/')}",
        "post_link":        (listing.get("post_link") or "").strip(),
        "post_location":    (listing.get("post_location") or "").strip(),
        "class_rates":      (listing.get("class_rates") or "").strip(),
        "grades":           resolve_tags(listing.get("grades", "")),
        "delivery_method":  (listing.get("delivery_method") or "").strip().rstrip("_"),
        "format":           (listing.get("format") or "").strip(),
        "duration":         (listing.get("duration") or "").strip(),
        "scheduling":       (listing.get("scheduling") or "").strip(),
        "prerequisites":    (listing.get("prerequisites") or "").strip(),
        "cohort_size":      listing.get("cohort_size"),
        "listing_category": (listing.get("listing_category") or "").strip(),
        "last_updated":     listing.get("revision_timestamp", ""),
        "city":             city,
        "state":            state,
        "country":          country,
    }

    return record


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY)
    index  = client.init_index(ALGOLIA_INDEX_NAME)

    # Step 1: Get all active user IDs via POST user/search
    print("Fetching active user IDs…")
    user_ids = get_all_user_ids()
    # Deduplicate
    user_ids = list(dict.fromkeys(user_ids))
    print(f"  {len(user_ids)} active users found\n")

    educator_records = []
    listing_records  = []

    # Step 2: For each user, fetch full profile + listings
    for i, uid in enumerate(user_ids, 1):
        print(f"[{i}/{len(user_ids)}] user_id={uid}")

        # Full profile
        user = get_user(uid)
        if not user:
            print(f"  user/get returned nothing, skipping")
            time.sleep(0.3)
            continue

        educator_records.append(enforce_byte_cap(build_educator_record(user)))

        # Listings
        try:
            all_listings = get_user_listings(uid)
            published = [
                l for l in all_listings
                if str(l.get("group_status")) == LISTING_STATUS
                and str(l.get("data_id")) == LISTING_DATA_ID
            ]
            for listing in published:
                listing_records.append(enforce_byte_cap(build_listing_record(listing)))
            print(f"  OK — {len(published)} published listings")
        except Exception as e:
            print(f"  listings error: {e}")

        time.sleep(0.4)

    print(f"\n{len(educator_records)} educator records")
    print(f"{len(listing_records)} listing records")

    # Step 3: Push to Algolia
    all_records = educator_records + listing_records
    print(f"\nPushing {len(all_records)} records to '{ALGOLIA_INDEX_NAME}'…")

    BATCH = 500
    for i in range(0, len(all_records), BATCH):
        batch = all_records[i : i + BATCH]
        index.save_objects(batch)
        print(f"  Batch {i // BATCH + 1} saved ({len(batch)} records)")

    print("Sync complete.")


if __name__ == "__main__":
    main()
