#!/usr/bin/env python3
"""
bd_algolia_sync.py  v3.8
Syncs educators and listings to Algolia via BD API v2.

Strategy:
  1. Fetch user/search pages (JSON-wrapped HTML), parse JSON first,
     then extract member profile filenames from the decoded HTML string.
  2. For each filename: call user/get for full profile JSON.
  3. For each active user: call users_portfolio_groups/get for listings.
  4. Filter listings: group_status=1, data_id=6 (Classes & Resources).
  5. Push educators + listings to Algolia.

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

BD_HEADERS = {"X-Api-Key": BD_API_KEY}

LISTING_DATA_ID = "6"   # Classes & Resources
LISTING_STATUS  = "1"   # published
ACTIVE_USER     = "2"   # active member

MAX_RECORD_BYTES = 9_500
BIO_CHAR_LIMIT   = 500
SNIPPET_CHARS    = 205

NON_PROFILE_PREFIXES = (
    "api/", "pictures/", "photos/", "images/", "about/",
    "checkout/", "contact/", "search/", "classes-and-resources",
    "login", "register", "dashboard", "profile/",
)


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


def bd_get_all_for_user(endpoint: str, user_id: str) -> list:
    """Fetch all records for a user_id, handling pagination."""
    results = []
    page = 1
    while True:
        data = bd_get(endpoint, params={
            "property":       "user_id",
            "property_value": user_id,
            "page":           page,
            "limit":          100,
        })
        msg = data.get("message") or []
        if not isinstance(msg, list) or not msg:
            break
        results.extend(msg)
        total_pages = int(data.get("total_pages") or 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.4)
    return results


# ── User discovery ────────────────────────────────────────────────────────────

def get_profile_filenames() -> list:
    """
    Fetch all pages of user/search (returns JSON-wrapped HTML).
    Parse the JSON, extract the HTML from the 'message' field,
    then regex-extract member profile filenames.

    Profile URLs in the decoded HTML look like:
      href="https://learn.everyavenue.com/canada/toronto/1-on-1-teaching/kt-tt"
    Filename = path after domain:
      canada/toronto/1-on-1-teaching/kt-tt
    """
    filenames = set()
    page = 1

    while True:
        resp = requests.get(
            f"{BD_BASE_URL}/user/search",
            headers=BD_HEADERS,
            params={"page": page, "limit": 100},
            timeout=30,
        )
        resp.raise_for_status()

        # Parse as JSON — the HTML lives in response["message"]
        data = resp.json()
        html = data.get("message", "")
        total_pages = int(data.get("total_pages") or 1)

        if not isinstance(html, str):
            print(f"  page {page}: unexpected message type {type(html)}")
            break

        # Extract member profile hrefs from the decoded HTML
        raw_hrefs = re.findall(
            rf'href="{re.escape(BD_BASE)}/([^"#]+)"',
            html,
        )

        for href in raw_hrefs:
            href = href.strip().rstrip("/")
            if any(href.startswith(p) for p in NON_PROFILE_PREFIXES):
                continue
            if href.endswith("/connect") or href.endswith("/message"):
                continue
            if "/" not in href:
                continue
            filenames.add(href)

        print(f"  HTML page {page}/{total_pages}: {len(raw_hrefs)} hrefs, {len(filenames)} unique profiles so far")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    return list(filenames)


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

    profile_photo = user.get("profile_photo") or ""
    if profile_photo and not profile_photo.startswith("http"):
        profile_photo = f"{BD_BASE}/{profile_photo}"

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
        "profile_url":        f"{BD_BASE}/{user.get('filename', '')}".strip(),
        "profile_photo":      profile_photo,
        "active":             user.get("active"),
        "signup_date":        user.get("signup_date", ""),
        "listing_type":       (user.get("listing_type") or "").strip(),
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
        "listing_url":      f"{BD_BASE}/{listing.get('group_filename', '')}".strip(),
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

    # Step 1: Discover member profile filenames
    print("Discovering member profiles from HTML pages…")
    filenames = get_profile_filenames()
    print(f"  {len(filenames)} unique profile paths found\n")

    # Step 2: Fetch JSON for each user + their listings
    educator_records = []
    listing_records  = []
    seen_user_ids    = set()

    for i, filename in enumerate(filenames, 1):
        print(f"[{i}/{len(filenames)}] {filename}")

        try:
            user_data = bd_get("/user/get", params={
                "property":       "filename",
                "property_value": filename,
            })
            msg  = user_data.get("message") or []
            user = msg[0] if isinstance(msg, list) and msg else {}
        except Exception as e:
            print(f"  user/get failed: {e}")
            continue

        if not user or str(user.get("active", "")) != ACTIVE_USER:
            print(f"  skipping (inactive or not found)")
            continue

        uid = str(user.get("user_id", ""))
        if uid in seen_user_ids:
            print(f"  duplicate user_id={uid}, skipping")
            continue
        seen_user_ids.add(uid)

        educator_records.append(enforce_byte_cap(build_educator_record(user)))

        try:
            all_listings = bd_get_all_for_user("/users_portfolio_groups/get", uid)
            published = [
                l for l in all_listings
                if str(l.get("group_status")) == LISTING_STATUS
                and str(l.get("data_id")) == LISTING_DATA_ID
            ]
            for listing in published:
                listing_records.append(enforce_byte_cap(build_listing_record(listing)))
            print(f"  OK — {len(published)} listings")
        except Exception as e:
            print(f"  listings failed: {e}")

        time.sleep(0.4)

    print(f"\n{len(educator_records)} active educators")
    print(f"{len(listing_records)} published listings")

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
