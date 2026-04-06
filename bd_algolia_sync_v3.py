#!/usr/bin/env python3
"""
bd_algolia_sync.py  v3.6
Syncs educators (users_data) and listings (data_posts) to Algolia
via the BD API v2 — no HTML scraping.

Key facts confirmed from debug:
  - user/search needs output_type=array or BD returns HTML
  - data_categories total=18; data_id=1 is "member_listings" (user type, not post)
  - data_posts/search rejects non-post data_ids with "Post Type not found"
  - 27 active members across 3 pages

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

BD_BASE_URL        = "https://learn.everyavenue.com/api/v2"
BD_API_KEY         = os.environ["BD_API_KEY"]
ALGOLIA_APP_ID     = os.environ["ALGOLIA_APP_ID"]
ALGOLIA_WRITE_KEY  = os.environ["ALGOLIA_WRITE_KEY"]
ALGOLIA_INDEX_NAME = os.environ.get("ALGOLIA_INDEX_NAME", "educators")

BD_HEADERS = {
    "X-Api-Key": BD_API_KEY,
    "Content-Type": "application/json",
}

MAX_RECORD_BYTES = 9_500
BIO_CHAR_LIMIT   = 500
SNIPPET_CHARS    = 205
PAGE_LIMIT       = 100

# BD system_names that belong to user profiles, not posts
# data_posts/search will reject these with "Post Type not found"
NON_POST_SYSTEM_NAMES = {
    "member_listings",
    "member_profile",
    "members",
}


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


def bd_search_all(endpoint: str, base_body: dict) -> list:
    """
    Paginate through a BD POST search endpoint.
    Results in response['message']; pagination via total_pages.
    """
    results = []
    page = 1

    while True:
        body = {**base_body, "page": page, "limit": PAGE_LIMIT}
        data = bd_post(endpoint, body)

        records = data.get("message") or []
        if not isinstance(records, list) or not records:
            break

        results.extend(records)
        print(f"    page {page}: {len(records)} records")

        total_pages = int(data.get("total_pages") or 1)
        if page >= total_pages:
            break

        page += 1
        time.sleep(0.7)

    return results


# ── Fetch post type IDs ───────────────────────────────────────────────────────

def fetch_post_type_ids() -> list:
    """
    Fetch data_categories sequentially (IDs 1-30).
    Returns only categories that are actual post types
    (skips member_listings and other user-profile types).
    Stops after 10 consecutive misses.
    """
    categories = []
    misses = 0

    for i in range(1, 31):
        try:
            data = bd_get(f"/data_categories/get/{i}")
            msg  = data.get("message")
            if not msg:
                misses += 1
                if misses >= 10:
                    break
                continue

            record = msg[0] if isinstance(msg, list) else msg
            if not record or not record.get("data_id"):
                misses += 1
                if misses >= 10:
                    break
                continue

            system_name = record.get("system_name", "")
            if system_name in NON_POST_SYSTEM_NAMES:
                print(f"  Skipping data_id={i} ({system_name}) — user profile type")
                misses = 0
                continue

            categories.append({
                "data_id":   str(record["data_id"]),
                "data_name": record.get("data_name", f"Type {i}"),
                "system_name": system_name,
            })
            misses = 0

        except Exception as e:
            misses += 1
            if misses >= 10:
                break

        time.sleep(0.2)

    return categories


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


def resolve_post_tags(post_tags_str: str) -> list:
    if not post_tags_str:
        return []
    return [t.strip() for t in post_tags_str.split(",") if t.strip()]


# ── Record builders ───────────────────────────────────────────────────────────

def build_educator_record(user: dict) -> dict:
    uid = str(user.get("user_id", ""))
    bio = strip_html(user.get("about_me") or "")[:BIO_CHAR_LIMIT]

    record = {
        "objectID":      f"educator_{uid}",
        "type":          "educator",
        "user_id":       uid,
        "name":          f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
        "company":       (user.get("company") or "").strip(),
        "bio":           bio,
        "city":          (user.get("city") or "").strip(),
        "state":         (user.get("state_ln") or "").strip(),
        "country":       (user.get("country_ln") or "").strip(),
        "website":       (user.get("website") or "").strip(),
        "profile_photo": (user.get("profile_photo") or "").strip(),
        "cover_photo":   (user.get("cover_photo") or "").strip(),
        "active":        user.get("active"),
        "signup_date":   user.get("signup_date", ""),
    }

    lat = user.get("lat")
    lon = user.get("lon")
    if lat and lon:
        try:
            record["_geoloc"] = {"lat": float(lat), "lng": float(lon)}
        except (ValueError, TypeError):
            pass

    return record


def build_listing_record(post: dict) -> dict:
    pid         = str(post.get("post_id") or "")
    title       = (post.get("post_title") or "").strip()
    raw_desc    = post.get("post_content") or ""
    description = strip_html(raw_desc)
    snippet     = description[:SNIPPET_CHARS]
    tags        = resolve_post_tags(post.get("post_tags", ""))

    raw_thumb = (post.get("post_image") or "").strip()
    thumbnail = (
        f"https://learn.everyavenue.com{raw_thumb}"
        if raw_thumb.startswith("/")
        else raw_thumb
    )

    category = ""
    if isinstance(post.get("data_category"), dict):
        category = post["data_category"].get("data_name", "")
    if not category:
        category = (post.get("category_name") or post.get("category") or "").strip()

    city = country = ""
    if isinstance(post.get("user"), dict):
        city    = post["user"].get("city", "")
        country = post["user"].get("country_ln", "")

    record = {
        "objectID":    f"listing_{pid}",
        "type":        "listing",
        "post_id":     pid,
        "user_id":     str(post.get("user_id") or ""),
        "title":       title,
        "description": description,
        "snippet":     snippet,
        "category":    category,
        "thumbnail":   thumbnail,
        "tags":        tags,
        "post_status": str(post.get("post_status", "")),
        "post_date":   post.get("post_live_date") or post.get("revision_timestamp", ""),
        "city":        city,
        "country":     country,
    }

    return record


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY)
    index  = client.init_index(ALGOLIA_INDEX_NAME)

    # ── Educators ─────────────────────────────────────────────────────────────
    print("Fetching educators…")
    all_users = bd_search_all(
        "/user/search",
        base_body={"output_type": "array", "action": "search"},
    )
    users = [u for u in all_users if str(u.get("active", "")) == "2"]
    print(f"  {len(users)} active educators (of {len(all_users)} total)")

    educator_records = [
        enforce_byte_cap(build_educator_record(u))
        for u in users
    ]

    # ── Post types ────────────────────────────────────────────────────────────
    print("Fetching post type IDs…")
    categories = fetch_post_type_ids()
    print(f"  Found {len(categories)} post types:")
    for c in categories:
        print(f"    data_id={c['data_id']}  {c['data_name']} ({c['system_name']})")

    # ── Listings ──────────────────────────────────────────────────────────────
    all_posts = []
    for cat in categories:
        data_id   = cat["data_id"]
        data_name = cat["data_name"]
        print(f"Fetching: {data_name} (data_id={data_id})…")
        try:
            posts = bd_search_all(
                "/data_posts/search",
                base_body={
                    "output_type": "array",
                    "action":      "search",
                    "data_id":     data_id,
                },
            )
            print(f"  {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"  Skipping ({e})")
        time.sleep(0.5)

    published = [p for p in all_posts if str(p.get("post_status", "")) == "1"]
    print(f"\n{len(published)} published listings (of {len(all_posts)} total)")

    listing_records = [
        enforce_byte_cap(build_listing_record(p))
        for p in published
    ]

    # ── Push to Algolia ───────────────────────────────────────────────────────
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
