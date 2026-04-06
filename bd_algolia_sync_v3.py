#!/usr/bin/env python3
"""
bd_algolia_sync.py  v3.1
Syncs educators (users_data) and listings (data_posts) to Algolia
via the BD API v2 — no HTML scraping.

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
    "Content-Type": "application/json",
    "X-Api-Key": BD_API_KEY,
}

MAX_RECORD_BYTES = 9_500
BIO_CHAR_LIMIT   = 500
SNIPPET_CHARS    = 205

ACTIVE_USER = 2   # BD enum: 2 = active member
ACTIVE_POST = 1   # BD enum: 1 = active post


# ── BD API helpers ────────────────────────────────────────────────────────────

def bd_get(endpoint: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{BD_BASE_URL}{endpoint}",
        headers=BD_HEADERS,
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def bd_post(endpoint: str, body: dict) -> dict:
    resp = requests.post(
        f"{BD_BASE_URL}{endpoint}",
        headers=BD_HEADERS,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def bd_search_all(endpoint: str, filters: dict, per_page: int = 100) -> list:
    """Paginate through a BD search endpoint and return all results."""
    results = []
    page = 1
    while True:
        body = {**filters, "page": page, "per_page": per_page}
        data = bd_post(endpoint, body)
        records = data.get("data") or data.get("results") or []
        if not records:
            break
        results.extend(records)
        if len(records) < per_page:
            break
        page += 1
        time.sleep(0.25)
    return results


# ── Tag helpers (optional — skipped gracefully if endpoints return nothing) ───

def fetch_tag_lookup() -> dict:
    """Returns {tag_id_str: tag_label}. Empty dict if endpoint unavailable."""
    try:
        data = bd_get("/tags_data")
        raw  = data.get("data") or []
        if not raw:
            print("  tags_data returned empty — skipping tag lookup")
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
    """Returns {object_id_str: [tag_label, ...]}. Empty dict if unavailable."""
    try:
        data = bd_get("/rel_tags", params={"object_type": object_type})
        raw  = data.get("data") or []
        if not raw:
            print(f"  rel_tags ({object_type}) returned empty — skipping")
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


def resolve_member_tags(member_tags_str: str, tag_lookup: dict) -> list:
    """Convert BD's comma-separated tag ID string to labels."""
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
    uid = str(user["user_id"])
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
        record["_geoloc"] = {"lat": float(lat), "lng": float(lon)}

    return record


def build_listing_record(post: dict, rel_tag_map: dict) -> dict:
    pid         = str(post.get("post_id") or post.get("id", ""))
    title       = (post.get("post_title") or post.get("title") or "").strip()
    raw_desc    = post.get("post_content") or post.get("description") or ""
    description = strip_html(raw_desc)
    snippet     = description[:SNIPPET_CHARS]
    tags        = rel_tag_map.get(pid, [])

    record = {
        "objectID":    f"listing_{pid}",
        "type":        "listing",
        "post_id":     pid,
        "user_id":     str(post.get("user_id") or ""),
        "title":       title,
        "description": description,
        "snippet":     snippet,
        "category":    (
            post.get("category_name")
            or post.get("data_cat_name")
            or post.get("category")
            or ""
        ).strip(),
        "thumbnail":   (
            post.get("post_image")
            or post.get("image")
            or post.get("thumbnail")
            or ""
        ).strip(),
        "tags":        tags,
        "active":      post.get("active"),
        "post_date":   post.get("post_date") or post.get("modtime", ""),
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

    # Educators
    print("Fetching active educators…")
    users = bd_search_all("/user/search", filters={"active": ACTIVE_USER})
    print(f"  {len(users)} educators found")

    educator_records = [
        enforce_byte_cap(build_educator_record(u, educator_tags, tag_lookup))
        for u in users
    ]

    # Listings
    print("Fetching active listings…")
    posts = bd_search_all("/data_posts/search", filters={"active": ACTIVE_POST})
    print(f"  {len(posts)} listings found")

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
