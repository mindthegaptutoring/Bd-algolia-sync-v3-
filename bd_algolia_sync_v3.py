#!/usr/bin/env python3
"""
bd_algolia_sync.py  v3.4
Syncs educators (users_data) and listings (data_posts) to Algolia
via the BD API v2 — no HTML scraping.

Uses POST /search endpoints with output_type=array and total_pages pagination.
Results live in response["message"], not response["data"].

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
PAGE_LIMIT       = 100   # BD max per page; stay under 100 req/min


# ── BD API helpers ────────────────────────────────────────────────────────────

def bd_post(endpoint: str, body: dict) -> dict:
    """Single POST request to BD API. Returns empty dict on empty response."""
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
    BD search responses wrap results in 'message' and use total_pages for pagination.
    """
    results = []
    page = 1

    while True:
        body = {**base_body, "page": page, "limit": PAGE_LIMIT}
        data = bd_post(endpoint, body)

        # BD search endpoints return results in "message"
        records = data.get("message") or []
        if not isinstance(records, list) or not records:
            break

        results.extend(records)
        print(f"    page {page}: {len(records)} records")

        total_pages = int(data.get("total_pages") or 1)
        if page >= total_pages:
            break

        page += 1
        time.sleep(0.7)   # stay under 100 req/min

    return results


# ── Tag helpers (optional — skipped gracefully if unavailable) ────────────────

def fetch_tag_lookup() -> dict:
    try:
        resp = requests.get(
            f"{BD_BASE_URL}/tags_data",
            headers=BD_HEADERS,
            timeout=30,
        )
        if not resp.text.strip():
            print("  tags_data empty — skipping")
            return {}
        data = resp.json()
        raw  = data.get("data") or data.get("message") or []
        if not raw:
            print("  tags_data empty — skipping")
            return {}
        return {
            str(t.get("id") or t.get("tag_id")): (
                t.get("tag_label") or t.get("name", "")
            ).strip()
            for t in raw
        }
    except Exception as e:
        print(f"  tags_data unavailable ({e}) — skipping")
        return {}


def fetch_rel_tags(object_type: str, tag_lookup: dict) -> dict:
    try:
        resp = requests.get(
            f"{BD_BASE_URL}/rel_tags",
            headers=BD_HEADERS,
            params={"object_type": object_type},
            timeout=30,
        )
        if not resp.text.strip():
            print(f"  rel_tags ({object_type}) empty — skipping")
            return {}
        data = resp.json()
        raw  = data.get("data") or data.get("message") or []
        if not raw or not isinstance(raw, list):
            print(f"  rel_tags ({object_type}) empty — skipping")
            return {}
        tag_map = {}
        for rel in raw:
            oid   = str(rel.get("object_id") or "")
            label = (
                rel.get("tag_label")
                or rel.get("tag_name")
                or tag_lookup.get(str(rel.get("tag_id")), "")
            ).strip()
            if oid and label:
                tag_map.setdefault(oid, []).append(label)
        return tag_map
    except Exception as e:
        print(f"  rel_tags ({object_type}) unavailable ({e}) — skipping")
        return {}


def resolve_post_tags(post_tags_str: str) -> list:
    """
    BD stores post_tags as a comma-separated string: "tag1,tag2,tag3"
    Convert directly to a list — no lookup needed.
    """
    if not post_tags_str:
        return []
    return [t.strip() for t in post_tags_str.split(",") if t.strip()]


def resolve_member_tags(member_tags_str: str, tag_lookup: dict) -> list:
    if not member_tags_str or not tag_lookup:
        return []
    ids = [t.strip() for t in member_tags_str.split(",") if t.strip()]
    return [tag_lookup[i] for i in ids if i in tag_lookup]


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


# ── Record builders ───────────────────────────────────────────────────────────

def build_educator_record(user: dict, rel_tag_map: dict, tag_lookup: dict) -> dict:
    uid = str(user.get("user_id", ""))
    bio = strip_html(user.get("about_me") or "")[:BIO_CHAR_LIMIT]

    tags = (
        rel_tag_map.get(uid)
        or resolve_member_tags(user.get("member_tags", ""), tag_lookup)
    )

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
        "tags":          tags,
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


def build_listing_record(post: dict, rel_tag_map: dict) -> dict:
    pid         = str(post.get("post_id") or "")
    title       = (post.get("post_title") or "").strip()
    raw_desc    = post.get("post_content") or ""
    description = strip_html(raw_desc)
    snippet     = description[:SNIPPET_CHARS]

    # post_tags comes as a comma-separated string in BD
    tags = (
        rel_tag_map.get(pid)
        or resolve_post_tags(post.get("post_tags", ""))
    )

    # Thumbnail: BD returns a relative path — prepend domain
    raw_thumb = (post.get("post_image") or "").strip()
    thumbnail = (
        f"https://learn.everyavenue.com{raw_thumb}"
        if raw_thumb.startswith("/")
        else raw_thumb
    )

    # Pull category name from nested data_category if present
    category = ""
    if isinstance(post.get("data_category"), dict):
        category = post["data_category"].get("data_name", "")
    if not category:
        category = (post.get("category_name") or post.get("category") or "").strip()

    # Pull educator location from nested user if present
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
        "post_status": post.get("post_status", ""),
        "post_date":   post.get("post_live_date") or post.get("revision_timestamp", ""),
        "city":        city,
        "country":     country,
    }

    return record


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY)
    index  = client.init_index(ALGOLIA_INDEX_NAME)

    # Tags — optional, skipped gracefully if unavailable
    print("Fetching tag definitions…")
    tag_lookup = fetch_tag_lookup()
    print(f"  {len(tag_lookup)} tags loaded")

    print("Fetching tag relationships…")
    educator_tags = fetch_rel_tags("user", tag_lookup)
    listing_tags  = fetch_rel_tags("post", tag_lookup)

    # Educators — POST /user/search
    print("Fetching educators…")
    all_users = bd_search_all(
        "/user/search",
        base_body={"output_type": "array", "action": "search"},
    )
    # Filter to active members only
    users = [u for u in all_users if str(u.get("active", "")) == "2"]
    print(f"  {len(users)} active educators (of {len(all_users)} total)")

    educator_records = [
        enforce_byte_cap(build_educator_record(u, educator_tags, tag_lookup))
        for u in users
    ]

    # Listings — POST /data_posts/search
    print("Fetching listings…")
    all_posts = bd_search_all(
        "/data_posts/search",
        base_body={"output_type": "array", "action": "search"},
    )
    # Filter to published posts only (post_status = "1")
    posts = [p for p in all_posts if str(p.get("post_status", "")) == "1"]
    print(f"  {len(posts)} published listings (of {len(all_posts)} total)")

    listing_records = [
        enforce_byte_cap(build_listing_record(p, listing_tags))
        for p in posts
    ]

    # Push to Algolia
    all_records = educator_records + listing_records
    print(f"Pushing {len(all_records)} records to '{ALGOLIA_INDEX_NAME}'…")

    BATCH = 500
    for i in range(0, len(all_records), BATCH):
        batch = all_records[i : i + BATCH]
        index.save_objects(batch)
        print(f"  Batch {i // BATCH + 1} saved ({len(batch)} records)")

    print("Sync complete.")


if __name__ == "__main__":
    main()
