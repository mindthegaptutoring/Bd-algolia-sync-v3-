#!/usr/bin/env python3
"""
bd_algolia_sync.py  v4.0
Syncs educators and listings to Algolia via BD API v2.

Key findings from debugging:
  - user/search HTML mode works but page param in body is ignored (always returns page 1)
  - user/search array mode returns empty with blank q
  - user/get by user_id works reliably for all users
  - users_portfolio_groups/get works WITHOUT page/limit params; returns 400 with them
  - Pagination for portfolio_groups uses next_page cursor token, not numeric pages

Strategy:
  1. POST user/search (HTML) to get total_members count
  2. Probe user IDs 1..MAX sequentially via user/get to find all active users
     (stops once we've found total_members active users)
  3. For each user: GET users_portfolio_groups (no extra params) for their listings
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

LISTING_DATA_ID  = "6"   # Classes & Resources
LISTING_STATUS   = "1"   # published
ACTIVE_USER      = "2"   # active member
MAX_USER_ID      = 300   # probe up to this ID; increase as platform grows

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


# ── User discovery ────────────────────────────────────────────────────────────

def get_total_member_count() -> int:
    """
    POST user/search (HTML mode) to extract total_members from the JSON envelope.
    Even though the message body is HTML, the count is always in the JSON.
    """
    try:
        data = bd_post("/user/search", {"limit": 1})
        return int(data.get("total_members") or 0)
    except Exception as e:
        print(f"  Could not get member count: {e}")
        return MAX_USER_ID   # fall back to probing everything


def get_all_active_users(total_members: int) -> list:
    """
    Probe user IDs sequentially from 1 to MAX_USER_ID.
    Returns list of full user dicts for active members (active=2).
    Stops early once we've found total_members active users.
    """
    users = []
    consecutive_misses = 0

    for uid in range(1, MAX_USER_ID + 1):
        try:
            data = bd_get("/user/get", params={
                "property":       "user_id",
                "property_value": str(uid),
            })
            msg  = data.get("message") or []
            user = msg[0] if isinstance(msg, list) and msg else None

            if user:
                consecutive_misses = 0
                if str(user.get("active", "")) == ACTIVE_USER:
                    users.append(user)
                    print(f"  Found user_id={uid}: {user.get('first_name','')} {user.get('last_name','')}")
                    if len(users) >= total_members:
                        print(f"  Reached total_members={total_members}, stopping probe")
                        break
            else:
                consecutive_misses += 1
                # Stop after 50 consecutive misses once we have some users
                if len(users) > 0 and consecutive_misses >= 50:
                    print(f"  50 consecutive misses after finding {len(users)} users, stopping")
                    break

        except Exception as e:
            consecutive_misses += 1

        time.sleep(0.25)   # stay well under 100 req/min

    return users


# ── Listing fetcher ───────────────────────────────────────────────────────────

def get_user_listings(user_id: str) -> list:
    """
    GET portfolio groups for a user.
    BD returns 400 (not empty array) when a user has no listings — treat as empty.
    Uses next_page cursor for pagination if needed.
    """
    all_listings = []
    page_cursor  = None

    while True:
        params = {
            "property":       "user_id",
            "property_value": user_id,
        }
        if page_cursor:
            params["page"] = page_cursor

        try:
            data = bd_get("/users_portfolio_groups/get", params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                break   # BD returns 400 when user has no listings
            raise
        msg  = data.get("message") or []

        if not isinstance(msg, list) or not msg:
            break

        all_listings.extend(msg)

        # Paginate using next_page cursor (BD uses cursor tokens, not page numbers)
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

    # Step 1: Get total member count
    print("Getting total member count…")
    total_members = get_total_member_count()
    print(f"  {total_members} total members on platform\n")

    # Step 2: Probe sequential user IDs to find all active members
    print(f"Probing user IDs 1-{MAX_USER_ID} for active members…")
    users = get_all_active_users(total_members)
    print(f"\n  {len(users)} active educators found\n")

    educator_records = []
    listing_records  = []

    # Step 3: Build records + fetch listings for each user
    for i, user in enumerate(users, 1):
        uid  = str(user.get("user_id", ""))
        name = f"{user.get('first_name','')} {user.get('last_name','')}".strip()
        print(f"[{i}/{len(users)}] {name} (user_id={uid})")

        educator_records.append(enforce_byte_cap(build_educator_record(user)))

        try:
            all_listings = get_user_listings(uid)
            published = [
                l for l in all_listings
                if str(l.get("group_status")) == LISTING_STATUS
                and str(l.get("data_id")) == LISTING_DATA_ID
            ]
            for listing in published:
                listing_records.append(enforce_byte_cap(build_listing_record(listing)))
            if published:
                print(f"  {len(published)} published listings")
            else:
                print(f"  no listings")
        except Exception as e:
            print(f"  listings error: {e}")

    print(f"\n{len(educator_records)} educator records")
    print(f"{len(listing_records)} listing records")

    # Step 4: Push to Algolia
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
