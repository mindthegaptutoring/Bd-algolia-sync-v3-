"""
reviews_sync.py
Version: 1.0.0

Refreshes reviews.json on its own, frequent schedule — deliberately
separate from listing_stats_sync.py's weekly cron. A new review should
show up on an educator's listing page in minutes, not wait up to 7 days
for the next weekly stats run.

This script is intentionally thin: all the real logic
(fetch_and_publish_reviews) lives in listing_stats_sync.py and is
imported from there, so there's one source of truth for how reviews get
fetched and published, not two copies that can drift apart. This file
just exists to give that one function its own fast cron schedule without
dragging the much heavier GSC/GA4 listing sync along with it.

REQUIRED ENV VARS (same BD_API_KEY / GitHub vars listing_stats_sync.py
already uses — no Google credentials needed, this script never touches
GSC or GA4):
  BD_API_KEY
  GITHUB_TOKEN
  GITHUB_REPO       (optional, defaults to mindthegaptutoring/lwea-search)
  GITHUB_BRANCH      (optional, defaults to main)

DEPLOYMENT: a separate Render Cron Job, same repo as listing_stats_sync.py
and backfill_history.py (this file must sit alongside listing_stats_sync.py
since it imports from it), running on a much shorter interval — every 15
minutes is a reasonable starting point given how lightweight this job is
(one BD API call, one small GitHub file write). Adjust freely; there's
no real cost to running this more often.

Build command (same as the other two scripts): pip install -r requirements.txt
Start command: python reviews_sync.py
Schedule: */15 * * * *
"""

from listing_stats_sync import fetch_and_publish_reviews

if __name__ == "__main__":
    print("Starting reviews sync...")
    fetch_and_publish_reviews()
    print("Done.")
